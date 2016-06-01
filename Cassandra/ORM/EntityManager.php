<?php

namespace CassandraBundle\Cassandra\ORM;

use Cassandra\BatchStatement;
use Cassandra\ExecutionOptions;
use Cassandra\Future;
use Cassandra\PreparedStatement;
use Cassandra\Session;
use Cassandra\Statement;
use CassandraBundle\Cassandra\Connection;
use CassandraBundle\Cassandra\ORM\FutureResponse;
use CassandraBundle\Cassandra\ORM\Mapping\ClassMetadataFactoryInterface;
use CassandraBundle\EventDispatcher\CassandraEvent;
use Psr\Log\LoggerInterface;

class EntityManager implements Session, EntityManagerInterface
{
    protected $connection;
    private $metadataFactory;
    private $logger;
    private $statements;

    const STATEMENT = 'statement';
    const ARGUMENTS = 'arguments';

    public function __construct(Connection $connection, ClassMetadataFactoryInterface $metadataFactory, LoggerInterface $logger)
    {
        $this->connection = $connection;
        $this->metadataFactory = $metadataFactory;
        $this->logger = $logger;
        $this->statements = [];
    }

    public function getConnection()
    {
        return $this->connection;
    }

    public function getKeyspace()
    {
        return $this->connection->getKeyspace();
    }

    /**
     * Gets the metadata factory used to gather the metadata of classes.
     *
     * @return \CassandraBundle\Cassandra\ORM\Mapping\ClassMetadataFactory
     */
    public function getMetadataFactory()
    {
        return $this->metadataFactory;
    }

    /**
     * Returns the ORM metadata descriptor for a class.
     *
     * The class name must be the fully-qualified class name without a leading backslash
     * (as it is returned by get_class($obj)) or an aliased class name.
     *
     * Examples:
     * MyProject\Domain\User
     * sales:PriceRequest
     *
     * Internal note: Performance-sensitive method.
     *
     * @param string $className
     *
     * @return \CassandraBundle\Cassandra\ORM\Mapping\ClassMetadata
     */
    public function getClassMetadata($className)
    {
        return $this->metadataFactory->getMetadataFor($className);
    }

    /**
     * Insert $entity to cassandra
     *
     * @param Object $entity
     * @return void
     */
    public function insert($entity)
    {
        $values = $this->readColumn($entity);
        $tableName = $this->getTableName($entity);
        $fields = array_keys($values);

        $statement = sprintf(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)",
            $this->getKeyspace(),
            $tableName,
            implode(', ', $fields),
            implode(', ', array_map(function () { return '?'; }, $fields))
        );

        $this->statements[] = [
            self::STATEMENT => $statement,
            self::ARGUMENTS => $values,
        ];
    }

    /**
     * Update $entity to cassandra
     *
     * @param Object $entity
     * @return void
     */
    public function update($entity)
    {
        $tableName = $this->getTableName($entity);
        $values = $this->readColumn($entity);
        $id = $values['id'];
        unset($values['id']);

        $fields = array_keys($values);

        $statement = sprintf(
            "UPDATE \"%s\".\"%s\" SET %s WHERE id = ?",
            $this->getKeyspace(),
            $tableName,
            implode(', ', array_map(function ($val) { return sprintf("%s = ?", $val); }, $fields))
        );

        $this->statements[] = [
            self::STATEMENT => $statement,
            self::ARGUMENTS => array_merge($values, ['id' => $id]),
        ];
    }

    /**
     * Delete $entity
     *
     * @param Object $entity
     * @return void
     */
    public function delete($entity)
    {
        $tableName = $this->getTableName($entity);

        $statement = sprintf(
            "DELETE FROM \"%s\".\"%s\" WHERE id = ?",
            $this->getKeyspace(),
            $tableName
        );

        $this->statements[] = [
            self::STATEMENT => $statement,
            self::ARGUMENTS => ['id' => new Cassandra\Uuid($entity->getId())],
        ];
    }

    /**
     * Execute batch process
     *
     * @return void
     */
    public function flush($async = true)
    {
        if (count($this->statements)) {
            $this->logger->debug('CASSANDRA: BEGIN');
            $batch = new BatchStatement(Cassandra::BATCH_LOGGED);

            foreach ($this->statements as $statement) {
                $this->logger->debug('CASSANDRA: '.$statement[self::STATEMENT].' => ['.implode(', ',$statement[self::ARGUMENTS]).']');
                if ($async) {
                    $batch->add($this->prepareAsync($statement[self::STATEMENT]), $statement[self::ARGUMENTS]);
                } else {
                    $batch->add($this->prepare($statement[self::STATEMENT]), $statement[self::ARGUMENTS]);
                }
            }

            if ($async) {
                $session->executeAsync($batch);
            } else {
                $session->execute($batch);
            }
            $this->logger->debug('CASSANDRA: END');
            $this->statements = [];
        }
    }

    /**
     * Return values of all column in an $entity
     *
     * @param Object $entity
     * @return []
     */
    private function readColumn($entity)
    {
        $values = [];
        $reflectionClass = new \ReflectionClass($entity);

        foreach ($reflectionClass->getProperties() as $reflectionProperty) {
            $columnAnnotation = $this->reader->getPropertyAnnotation($reflectionProperty, self::ANNOTATION_CASSANDRA_COLUMN_CLASS);
            if ($columnAnnotation) {
                $value = $entity->{'get'.ucwords($reflectionProperty->name)}();
                if ($value) {
                    $values[$columnAnnotation->name] = $this->encodeColumnType($columnAnnotation->type, $value);
                }
            }
        }

        return $values;
    }

    /**
     * Return $value with appropriate $type
     *
     * @param string $type
     * @param mixed $value
     * @return mixed
     */
    private function encodeColumnType($type, $value)
    {
        switch ($type) {
            case 'uuid':
                return new Cassandra\Uuid($value);
                break;
            case 'float':
                return new Cassandra\Float($value);
            default:
                return $value;
                break;
        }
    }

    /**
     * Return table name of $entity
     *
     * @param Object $entity
     * @return string
     */
    private function getTableName($entity)
    {
        $tableName = (new \ReflectionClass($entity))->getShortName();

        return strtolower(preg_replace('/([^A-Z])([A-Z])/', "$1_$2", $tableName));
    }

    /**
     * Executes a given statement and returns a result
     *
     * @param Statement        $statement statement to be executed
     * @param ExecutionOptions $options   execution options
     *
     * @throws \Cassandra\Exception
     *
     * @return \Cassandra\Rows execution result
     */
    public function execute(Statement $statement, ExecutionOptions $options = null)
    {
        return $this->send('execute', [$statement, $options]);
    }

    /**
     * Executes a given statement and returns a future result
     *
     * Note that this method ignores ExecutionOptions::$timeout option, you can
     * provide one to Future::get() instead.
     *
     * @param Statement        $statement statement to be executed
     * @param ExecutionOptions $options   execution options
     *
     * @return \Cassandra\Future     future result
     */
    public function executeAsync(Statement $statement, ExecutionOptions $options = null)
    {
        return $this->send('executeAsync', [$statement, $options]);
    }

    /**
     * Creates a prepared statement from a given CQL string
     *
     * Note that this method only uses the ExecutionOptions::$timeout option,
     * all other options will be ignored.
     *
     * @param string           $cql     CQL statement string
     * @param ExecutionOptions $options execution options
     *
     * @throws \Cassandra\Exception
     *
     * @return PreparedStatement  prepared statement
     */
    public function prepare($cql, ExecutionOptions $options = null)
    {
        return $this->send('prepare', [$cql, $options]);
    }

    /**
     * Asynchronously prepares a statement and returns a future prepared statement
     *
     * Note that all options passed to this method will be ignored.
     *
     * @param string           $cql     CQL string to be prepared
     * @param ExecutionOptions $options preparation options
     *
     * @return \Cassandra\Future  statement
     */
    public function prepareAsync($cql, ExecutionOptions $options = null)
    {
        return $this->send('prepareAsync', [$cql, $options]);
    }

    /**
     * Closes current session and all of its connections
     *
     * @param float|null $timeout Timeout to wait for closure in seconds
     *
     * @return void
     */
    public function close($timeout = null)
    {
        $this->connection->getSession()->close($timeout);
        $this->connection->resetSession();
    }

    /**
     * Asynchronously closes current session once all pending requests have finished
     *
     * @return \Cassandra\Future  future
     */
    public function closeAsync()
    {
        $this->connection->getSession()->closeAsync();
        $this->connection->resetSession();
    }

    /**
     * Returns current schema.
     *
     * NOTE: the returned Schema instance will not be updated as the actual
     *       schema changes, instead an updated instance should be requested by
     *       calling Session::schema() again.
     *
     * @return \Cassandra\Schema
     */
    public function schema()
    {
        return $this->connection->getSession()->schema();
    }

    /**
     * Prepare response to return
     *
     * @param mixed               $response
     * @param CassandraEvent|null $event
     *
     * @return mixed
     */
    protected function prepareResponse($response, CassandraEvent $event = null)
    {
        if (is_null($event)) {
            return $response;
        }

        if ($response instanceof Future) {
            return new FutureResponse($response, $event, $this->connection->getEventDispatcher());
        }

        $event->setExecutionStop();
        $this->connection->getEventDispatcher()->dispatch(CassandraEvent::EVENT_NAME, $event);

        return $response;
    }

    /**
     * Initialize event
     *
     * @param string $command
     * @param array  $args
     *
     * @return CassandraEvent|null Return null if no eventDispatcher available
     */
    protected function prepareEvent($command, array $args)
    {
        if (is_null($this->connection->getEventDispatcher())) {
            return null;
        }

        $event = new CassandraEvent();
        $event->setCommand($command)
            ->setKeyspace($this->connection->getKeyspace())
            ->setArguments($args)
            ->setExecutionStart();

        return $event;
    }

    /**
     * Send command to cassandra session
     *
     * @param string $command
     * @param array  $arguments
     *
     * @return mixed
     */
    protected function send($command, array $arguments)
    {
        $event = $this->prepareEvent($command, $arguments);

        // The last arguments of call_user_func_array must not be null
        if (end($arguments) === null) {
            array_pop($arguments);
        }

        $retry = $this->connection->getMaxRetry();
        while ($retry >= 0) {
            try {
                $return = call_user_func_array([$this->connection->getSession(), $command], $arguments);

                // No exception, we can return the result
                $retry = -1;
            } catch (\Cassandra\Exception\RuntimeException $e) {
                if ($retry > 0) {
                    // Reset the current session to retry the command
                    $this->connection->resetSession();
                    $retry--;
                } else {
                    // too many retries, rethrow the exception
                    throw $e;
                }
            }
        }

        return $this->prepareResponse($return, $event);
    }
}
