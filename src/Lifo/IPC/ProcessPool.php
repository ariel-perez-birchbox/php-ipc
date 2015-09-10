<?php
/**
 * This file is part of the Lifo\IPC PHP Library.
 *
 * (c) Jason Morriss <lifo2013@gmail.com>
 *
 * Copyright (c) 2013 Jason Morriss
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished
 * to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace Lifo\IPC;
declare(ticks = 1);

/**
 * A simple Process Pool manager for managing a list of forked processes.
 * API Inspired by the Python multiprocessing library.
 *
 * Each "process" is a function that is called via a forked child. A child
 * may send any serializable result back to the parent or can use
 * ProcessPool::socket_send() to send multiple serializable results to the
 * $parent socket.
 *
 * Only 1 ProcessPool per process can be active at a time due to the signal
 * handler for SIGCHLD. IF you attempt to start a second pool within the same
 * process the second instance will override the SIGCHLD handler and the
 * previous ProcessPool will not reap its children properly.
 *
 * @example
$pool = new ProcessPool(16);
 * for ($i=0; $i<100; $i++) {
 * $pool->apply(function($parent) use ($i) {
 * echo "$i running...\n";
 * mt_srand(); // must re-seed for each child
 * $rand = mt_rand(1000000, 2000000);
 * usleep($rand);
 * return $i . ' : slept for ' . ($rand / 1000000) . ' seconds';
 * });
 * }
 * while ($pool->getPending()) {
 * try {
 * $result = $pool->get(1);    // timeout in 1 second
 * echo "GOT: ", $result, "\n";
 * } catch (ProcessPoolException $e) {
 * // timeout
 * }
 * }
 *
 */
class ProcessPool
{
    /** @var Integer Maximum workers allowed at once */
    protected $max;
    /** @var Integer Total results collected */
    protected $count;
    /** @var array Pending processes that have not been started yet */
    protected $pending;
    /** @var array Processes that have been started */
    protected $workers;
    /** @var array Results that have been collected */
    protected $results;
    /** @var \Closure Function to call every time a child is forked */
    protected $createCallback;
    /** @var array children PID's that died prematurely */
    private $caught;
    /** @var boolean Is the signal handler initialized? */
    private $initialized;
    /** @var string path to calling script */
    private $callerPath = '/tmp';
    /** @var resource message queue */
    private $messageQueue;
    /** @var integer queue max byte capacity */
    private $queueMaxBytes = 0;
    /** @var integer size of average message in bytes */
    private $averageMessageSize = 2048;

    public function __construct($max = 1, $callerPath = '/tmp')
    {
        $this->count = 0;
        $this->max = $max;
        $this->results = array();
        $this->workers = array();
        $this->pending = array();
        $this->caught = array();
        $this->initialized = false;
        $this->callerPath = $callerPath;
        msg_remove_queue(msg_get_queue($this->generateQueueId()));
    }

    public function __destruct()
    {
        // make sure signal handler is removed
        $this->uninit();
    }

    /**
     * Initialize the signal handler.
     *
     * Note: This will replace any current handler for SIGCHLD.
     *
     * @param boolean $force Force initialization even if already initialized
     */
    private function init($force = false)
    {
        if ($this->initialized and !$force) {
            return;
        }

        $this->messageQueue = msg_get_queue($this->generateQueueId());
        $this->initialized = true;
        pcntl_signal(SIGCHLD, array($this, 'signalHandler'));
    }

    private function generateQueueId()
    {
        // For some reason this number has to be lower than 256KB
        // otherwise, the queue can't be created.
        // Don't know the exact limit, but 128KB works
        return ftok($this->callerPath, "A") % 131072;
    }

    private function uninit()
    {
        if (!$this->initialized) {
            return;
        }
        $this->initialized = false;

        pcntl_signal(SIGCHLD, SIG_DFL);
    }

    public function signalHandler($signo)
    {
        switch ($signo) {
            case SIGCHLD:
                $this->reaper();
                break;
        }
    }

    /**
     * Reap any dead children
     */
    public function reaper($pid = null, $status = null)
    {
        if ($pid === null) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
        }
        while ($pid > 0) {
            if (isset($this->workers[$pid])) {
                unset($this->workers[$pid]);
            } else {
                // the child died before the parent could initialize the $worker
                // queue. So we track it temporarily so we can handle it in
                // self::create().
                $this->caught[$pid] = $status;
            }
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
        }
    }

    /**
     * Wait for any child to be ready
     *
     * @param integer $timeout Timeout to wait (fractional seconds)
     * @return integer|null Returns number of results available for reading
     */
    public function wait($timeout = null)
    {
        $startTime = microtime(true);
        while (true) {
            $this->apply();                         // maintain worker queue
            $msgCount = $this->getQueueDepth();
            if ($msgCount) {
                return $msgCount;
            }
            // timed out?
            if ($timeout and microtime(true) - $startTime > $timeout) {
                return null;
            }
            // no sense in waiting if we have no workers and no more pending
            if (empty($this->workers) and empty($this->pending)) {
                return null;
            }
        }
    }

    /**
     * Process all results in the message queue
     *
     * @return boolean| Returns whether or not it processed anything
     */
    public function processResults()
    {
        $processed = false;
        while ($res = $this->popMessage()) {
            $this->results[] = $res;
            $this->count++;
            $processed = true;
        }
        return $processed;
    }

    /**
     * Return the next available result.
     *
     * Blocks unless a $timeout is specified.
     *
     * @param integer $timeout Timeout in fractional seconds if no results are available.
     * @return mixed Returns next child response or null on timeout
     * @throws ProcessPoolException On timeout if $nullOnTimeout is false
     */
    public function get($timeout = null, $nullOnTimeout = false)
    {
        $startTime = microtime(true);
        while ($this->getPending()) {
            // return the next result
            if ($this->hasResult()) {
                return $this->getResult();
            }
            // wait for the next result
            if ($this->wait($timeout)) {
                $this->processResults();
            }
            if ($this->hasResult()) {
                return $this->getResult();
            }
            // timed out?
            if ($timeout and microtime(true) - $startTime > $timeout) {
                if ($nullOnTimeout) {
                    return null;
                }
                throw new ProcessPoolException("Timeout");
            }
        }
    }

    /**
     * Return results from all workers.
     *
     * Does not return until all pending workers are complete or the $timeout
     * is reached.
     *
     * @param integer $timeout Timeout in fractional seconds if no results are available.
     * @return array Returns an array of results
     * @throws ProcessPoolException On timeout if $nullOnTimeout is false
     */
    public function getAll($timeout = null, $resultsOnTimeout = false)
    {
        $results = array();
        $startTime = microtime(true);
        while ($this->getPending()) {
            try {
                $res = $this->get($timeout);
                if ($res !== null) {
                    $results[] = $res;
                }
            } catch (ProcessPoolException $e) {
                //Timed Out
            }

            // timed out?
            if ($timeout and microtime(true) - $startTime > $timeout) {
                if ($resultsOnTimeout) {
                    return $results;
                }
                throw new ProcessPoolException("Timeout");
            }
        }
        return $results;
    }

    public function hasResult()
    {
        return !empty($this->results);
    }

    /**
     * Return the next available result or null if none are available.
     *
     * This does not wait or manage the worker queue.
     */
    public function getResult()
    {
        if (empty($this->results)) {
            return null;
        }
        return array_shift($this->results);
    }

    /**
     * Apply a worker to the working or pending queue
     *
     * @param Callable $func Callback function to fork into.
     * @return ProcessPool
     */
    public function apply($func = null)
    {
        // add new function to pending queue
        if ($func !== null) {
            if ($func instanceof \Closure or $func instanceof ProcessInterface or is_callable($func)) {
                $this->pending[] = func_get_args();
            } else {
                throw new \UnexpectedValueException("Parameter 1 in ProcessPool#apply must be a Closure or callable");
            }
        }
        // start new workers if our current worker queue is low
        while (!empty($this->pending) and count($this->workers) < $this->max) {
            call_user_func_array(array($this, 'create'), array_shift($this->pending));
        }
        return $this;
    }

    /**
     * Create a new worker.
     *
     *
     * @param Closure $func Callback function.
     * @param mixed Any extra parameters are passed to the callback function.
     * @throws \RuntimeException if the child can not be forked.
     */
    protected function create($func /*, ...*/)
    {
        $args = array_slice(func_get_args(), 1);
        $this->init();                  // make sure signal handler is installed

        $pid = pcntl_fork();
        if ($pid == -1) {
            throw new \RuntimeException("Could not fork");
        }
        if ($pid > 0) {
            // PARENT PROCESS; Just track the child and return
            $this->workers[$pid] = $pid;
            $this->doOnCreate($args, 1);
            // If a SIGCHLD was already caught at this point we need to
            // manually handle it to avoid a defunct process.
            if (isset($this->caught[$pid])) {
                $this->reaper($pid, $this->caught[$pid]);
                unset($this->caught[$pid]);
            }

            // Process results if the queue exceeds approximately 80% capacity.
            $msgCount = $this->getQueueDepth();
            $queueMaxBytes = $this->getQueueMaxBytes();
            $avgMessageSize = $this->getAverageMessageSize();

            if ($msgCount * $avgMessageSize >= $queueMaxBytes * 0.8) {
                $this->processResults();
            }
        } else {
            // CHILD PROCESS; execute the callback function and wait for response
            try {
                if ($func instanceof ProcessInterface) {
                    $result = call_user_func_array(array($func, 'run'), $args);
                } else {
                    $result = call_user_func_array($func, $args);
                }
                if ($result !== null) {
                    $this->pushMessage($result);
                }
            } catch (\Exception $e) {
                // this is kind of useless in a forking context but at
                // least the developer can see the exception if it occurs.
                throw $e;
            }
            exit(0);
        }

    }

    /**
     * Clear all pending workers from the queue.
     */
    public function clear()
    {
        $this->pending = array();
        return $this;
    }

    /**
     * Send a SIGTERM (or other) signal to the PID given
     */
    public function kill($pid, $signo = SIGTERM)
    {
        posix_kill($pid, $signo);
        return $this;
    }

    /**
     * Send a SIGTERM (or other) signal to all current workers
     */
    public function killAll($signo = SIGTERM)
    {
        foreach ($this->workers as $pid) {
            $this->kill($pid, $signo);
        }
        return $this;
    }

    /**
     * Set a callback when a new forked process is created. This will allow the
     * parent to perform some sort of cleanup after every child is created.
     *
     * This is useful to reinitialize certain resources like DB connections
     * since children will inherit the parent resources.
     *
     * @param \Closure $callback Function to callback after every forked child.
     */
    public function setOnCreate(\Closure $callback = null)
    {
        $this->createCallback = $callback;
    }

    protected function doOnCreate($args = array())
    {
        if ($this->createCallback) {
            call_user_func_array($this->createCallback, $args);
        }
    }

    /**
     * Return the total jobs that have NOT completed yet.
     */
    public function getPending($pendingOnly = false)
    {
        if ($pendingOnly) {
            return count($this->pending);
        }
        return count($this->pending) + count($this->workers) + count($this->results);
    }

    public function getWorkers()
    {
        return count($this->workers);
    }

    public function getActive()
    {
        return count($this->pending) + count($this->workers);
    }

    public function getCompleted()
    {
        return $this->count;
    }

    public function setMax($max)
    {
        if (!is_numeric($max) or $max < 1) {
            throw new \InvalidArgumentException("Max value must be > 0");
        }
        $this->max = $max;
        return $this;
    }

    public function getMax()
    {
        return $this->max;
    }

    /**
     * Write the data to the message queue.
     */
    public function pushMessage($data)
    {
        msg_send($this->messageQueue, 1, $data, true, false);
    }

    /**
     * Read a data packet from the message queue.
     *
     * Blocking.
     *
     */
    public function popMessage()
    {
        $msgType = null;
        $data = null;
        $errorCode = null;

        //TODO: Handle Error Codes
        msg_receive($this->messageQueue, 1, $msgType, 4096, $data, true, MSG_NOERROR, $errorCode);
        $this->updateAverageMessageSize($data);

        return $data;
    }

    /**
     * Return number of messages in queue
     */
    public function getQueueDepth()
    {
        $stats = msg_stat_queue($this->messageQueue);
        return $stats['msg_qnum'];
    }

    /**
     * Return queue max capacity in bytes
     *
     */
    public function getQueueMaxBytes()
    {
        if ($this->queueMaxBytes == 0) {
            $stats = msg_stat_queue($this->messageQueue);
            $this->queueMaxBytes = $stats['msg_qbytes'];
        }
        return $this->queueMaxBytes;
    }

    /**
     * Return average message size, in bytes
     *
     */
    public function getAverageMessageSize()
    {
        return $this->averageMessageSize;
    }

    /**
     * Updates average message size
     *
     */
    public function updateAverageMessageSize($message)
    {
        $size = strlen(serialize($message));
        $totalBytesReceived = ($this->count * $this->averageMessageSize) + $size;

        $this->averageMessageSize = ceil($totalBytesReceived / ($this->count + 1.0));
    }
}