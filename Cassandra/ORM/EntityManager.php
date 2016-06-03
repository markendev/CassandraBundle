<?php

namespace CassandraBundle\Cassandra\ORM;

use Cassandra\BatchStatement;
use Cassandra\ExecutionOptions;
use Cassandra\Session;
use Cassandra\Statement;
use CassandraBundle\Cassandra\Connection;
use CassandraBundle\Cassandra\ORM\SchemaManager;
use CassandraBundle\Cassandra\ORM\Mapping\ClassMetadataFactoryInterface;
use CassandraBundle\EventDispatcher\CassandraEvent;
use Psr\Log\LoggerInterface;

class EntityManager implements Session, EntityManagerInterface
{
    protected $connection;
    private $metadataFactory;
    private $logger;
    private $statements;
    private $schemaManager;

    const STATEMENT = 'statement';
    const ARGUMENTS = 'arguments';

    public function __construct(Connection $connection, ClassMetadataFactoryInterface $metadataFactory, LoggerInterface $logger)
    {
        $this->connection = $connection;
        $this->metadataFactory = $metadataFactory;
        $this->logger = $logger;
        $this->statements = [];
        $this->schemaManager = new SchemaManager($connection);
    }

    public function getConnection()
    {
        return $this->connection;
    }

    public function getKeyspace()
    {
        return $this->connection->getKeyspace();
    }

    public function getSchemaManager()
    {
        return $this->schemaManager;
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
     * {@inheritDoc}
     */
    public function execute(Statement $statement, ExecutionOptions $options = null)
    {
        return $this->connection->execute($statement, $options);
    }

    /**
     * {@inheritDoc}
     */
    public function executeAsync(Statement $statement, ExecutionOptions $options = null)
    {
        return $this->connection->executeAsync($statement, $options);
    }

    /**
     * {@inheritDoc}
     */
    public function prepare($cql, ExecutionOptions $options = null)
    {
        return $this->connection->prepare($cql, $options);
    }

    /**
     * {@inheritDoc}
     */
    public function prepareAsync($cql, ExecutionOptions $options = null)
    {
        return $this->connection->prepareAsync($cql, $options);
    }

    /**
     * {@inheritDoc}
     */
    public function close($timeout = null)
    {
        return $this->connection->close($timeout);
    }

    /**
     * {@inheritDoc}
     */
    public function closeAsync()
    {
        return $this->connection->closeAsync();
    }

    /**
     * {@inheritDoc}
     */
    public function schema()
    {
        return $this->connection->schema();
    }

    /**
     * {@inheritDoc}
     */
    protected function prepareResponse($response, CassandraEvent $event = null)
    {
        return $this->connection->prepareResponse($response, $event);
    }

    /**
     * {@inheritDoc}
     */
    protected function prepareEvent($command, array $args)
    {
        return $this->connection->prepareEvent($command, $args);
    }

    /**
     * {@inheritDoc}
     */
    protected function send($command, array $arguments)
    {
        return $this->connection->send($command, $arguments);
    }
}
