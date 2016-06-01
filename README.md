# CassandraBundle

The CassandraBundle provide a Cassandra EntityManager as a Symfony service.

## Installation

**NOTE :** You need to [install the offical datastax php driver extension](https://github.com/datastax/php-driver)


Require the bundle in your composer.json file :

```json
{
    "require": {
        "hendrahuang/cassandra-bundle": "dev-master",
    }
}
```

Register the bundle in your kernel :

```php
// app/AppKernel.php

public function registerBundles()
{
    $bundles = array(
        new CassandraBundle\CassandraBundle(),
    );
}
```

Then install the bundle :

```shell
$ composer update hendrahuang/cassandra-bundle
```

## Usage

Add the `cassandra` section in your configuration file. Here is the minimal configuration required. 

```yaml
cassandra:
    connections:
        default:
            keyspace: "mykeyspace"
            hosts:
                - 127.0.0.1
                - 127.0.0.2
                - 127.0.0.3
            user: ''
            password: ''
        
```

Then you can ask container for your entity manager :

```php
$em = $this->get('cassandra.default_entity_manager');

// Insertion
$em->insert($entity);

// Update
$em->update($entity);

// Deletion
$em->delete($entity);

$em->flush();
```

Bundle provide a util class for extracting Datetime from a timeuuid string. 

```php
use CassandraBundle\Cassandra\Utility\Type as TypeUtils;

$datetime = TypeUtils::getDateTimeFromTimeuuidString('513a5340-6da0-11e5-815e-93ec150e89fd');

if (is_null($datetime)) {
    // something is wrong with supplied uuid
} else {
    echo $datetime->format(\DateTime::W3C); // 2015-10-08 11:38:22+02:00
}
```

## DataCollector

Datacollector is available when the symfony profiler is enabled. The collector allows you to see the following Cassandra data :

- keyspace
- command name
- command arguments
- execution time
- execution options override (consistency, serial consistency, page size and timeout)

**NOTE :** The time reported in the data collector may not be the real execution time in case you use the async calls : `executeAsync` and `prepareAsync`

## Configuration reference

```yaml
cassandra:
    dispatch_events: true                 # By default event are triggered on each cassandra command
    connections:
        default:
            persistent_sessions: true     # persistent session connection 
            keyspace: "mykeyspace"        # required keyspace to connect
            load_balancing: "round-robin" # round-robin or dc-aware-round-robin
            dc_options:                   # required if load balancing is set to dc-aware-round-robin
                local_dc_name: "testdc"
                host_per_remote_dc: 3
                remote_dc_for_local_consistency: false
            default_consistency: "one"    # 'one', 'any', 'two', 'three', 'quorum', 'all', 'local_quorum', 'each_quorum', 'serial', 'local_serial', 'local_one'
            default_pagesize: 10000       # -1 to disable pagination
            hosts:                        # required list of ip to contact
                - 127.0.0.1
            port: 9042                    # cassandra port
            token_aware_routing: true     # Enable or disable token aware routing
            user: ""                      # username for authentication
            password: ""                  # password for authentication
            ssl: false                    # set up ssl context
            default_timeout: null         # default is null, must be an integer if set
            timeout:
                connect: 5 
                request: 5 
            retries:
                sync_requests: 0          # Number of retries for synchronous requests. Default is 0, must be an integer if set

        client_name:
            ...
```

## Running the test

Install the composer dev dependencies

```shell
$ composer install --dev
```

Then run the test with [atoum](https://github.com/atoum/atoum) unit test framework
