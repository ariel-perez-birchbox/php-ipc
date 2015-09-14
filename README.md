PHP IPC Library
=======

**Inter Process Communication**

_This library is in its infancy. I am adding features to it as I require them in my other projects._

## Examples

### ProcessPool

The ProcessPool class provides a very simple interface to manage a list of
children that return 1 or more results and exit. Each child can return any value
that can be [serialized][serialize].

```php
<?php
use Lifo\IPC\ProcessPool;

// create a pool with a maximum of 16 workers at a time
$pool = new ProcessPool(16);

// apply 100 processes to the pool.
for ($i=0; $i<100; $i++) {
    $pool->apply(function() use ($i) {
        // Each child can optionally return a result
        mt_srand(); // must re-seed for each child
        $rand = mt_rand(1000000, 2000000);
        usleep($rand);
        return $i . ' : slept for ' . ($rand / 1000000) . ' seconds';
    });
}

// wait for all results to be finished ...
while ($pool->getPending()) {
    try {
        $result = $pool->get(1); // timeout in 1 second
        echo "GOT: ", $result, "\n";
    } catch (\Exception $e) {
        // timeout
    }
}

// easy shortcut to get all results at once.
$results = $pool->getAll();
```

#### Advanced ProcessPool Example

```php
<?php
use Lifo\IPC\ProcessPool;

// Pretend we have open resources like a DB connection...
$db = mysqli_connect(...);

// create a pool
$pool = new ProcessPool();
// set a callback that is called everytime a child is forked
$pool->setOnCreate(function() use (&$db) {
    // reconnect to the DB
    $db = mysqli_connect(...);
});

// create a child
$pool->apply(function() use (&$db) {
    // do stuff in the child; When we exit the $db handle will
    // close but won't affect the parent since it reconnected
    // after we were created (our $db handle is not equal to the parent $db)
    // ...
});

$results = $pool->getAll();
```

  [serialize]: http://php.net/serialize
