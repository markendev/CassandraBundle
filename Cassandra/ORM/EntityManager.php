<?php

namespace CassandraBundle\Cassandra\ORM;

use Cassandra\BatchStatement;
use Cassandra\ExecutionOptions;
use Cassandra\Session;
use Cassandra\Statement;
use CassandraBundle\Cassandra\Connection;
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
        $this->logger = $logger;
        $this->metadataFactory = $metadataFactory;
        $this->schemaManager = new SchemaManager($connection);
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
     * Insert $entity to cassandra.
     *
     * @param object $entity
     */
    public function insert($entity)
    {
        $metadata = $this->getClassMetadata(get_class($entity));
        $tableName = $metadata->table['name'];
        $values = $this->readColumn($entity, $metadata);
        $columns = array_keys($values);

        $statement = sprintf(
            'INSERT INTO "%s"."%s" (%s) VALUES (%s)',
            $this->getKeyspace(),
            $tableName,
            implode(', ', $columns),
            implode(', ', array_map(function () { return '?'; }, $columns))
        );

        $this->statements[] = [
            self::STATEMENT => $statement,
            self::ARGUMENTS => $values,
        ];
    }

    /**
     * Update $entity to cassandra.
     *
     * @param object $entity
     */
    public function update($entity)
    {
        $metadata = $this->getClassMetadata(get_class($entity));
        $tableName = $metadata->table['name'];
        $values = $this->readColumn($entity, $metadata);
        $id = $values['id'];
        unset($values['id']);
        $columns = array_keys($values);

        $statement = sprintf(
            'UPDATE "%s"."%s" SET %s WHERE id = ?',
            $this->getKeyspace(),
            $tableName,
            implode(', ', array_map(function ($column) { return sprintf('%s = ?', $column); }, $columns))
        );

        $this->statements[] = [
            self::STATEMENT => $statement,
            self::ARGUMENTS => array_merge($values, ['id' => $id]),
        ];
    }

    /**
     * Delete $entity.
     *
     * @param object $entity
     */
    public function delete($entity)
    {
        $metadata = $this->getClassMetadata(get_class($entity));
        $tableName = $metadata->table['name'];

        $statement = sprintf(
            'DELETE FROM "%s"."%s" WHERE id = ?',
            $this->getKeyspace(),
            $tableName
        );

        $this->statements[] = [
            self::STATEMENT => $statement,
            self::ARGUMENTS => ['id' => new \Cassandra\Uuid($entity->getId())],
        ];
    }

    /**
     * Execute batch process.
     */
    public function flush($async = true)
    {
        if (count($this->statements)) {
            $this->logger->debug('CASSANDRA: BEGIN');
            $batch = new BatchStatement(\Cassandra::BATCH_LOGGED);

            foreach ($this->statements as $statement) {
                $this->logger->debug('CASSANDRA: '.$statement[self::STATEMENT].' => ['.implode(', ', $statement[self::ARGUMENTS]).']');
                $batch->add($this->prepare($statement[self::STATEMENT]), $statement[self::ARGUMENTS]);
            }

            if ($async) {
                $this->executeAsync($batch);
            } else {
                $this->execute($batch);
            }
            $this->logger->debug('CASSANDRA: END');
            $this->statements = [];
        }
    }

    /**
     * Return values of all column in an $entity.
     *
     * @param object $entity
     *
     * @return []
     */
    private function readColumn($entity, $metadata)
    {
        foreach ($metadata->fieldMappings as $field) {
            $getterMethod = 'get'.ucfirst($field['fieldName']);
            $values[$field['columnName']] = $this->encodeColumnType($field['type'], $entity->{$getterMethod}());
        }

        return $values;
    }

    /**
     * Return $value with appropriate $type.
     *
     * @param string $type
     * @param mixed  $value
     *
     * @return mixed
     */
    private function encodeColumnType($type, $value)
    {
        switch ($type) {
            case 'uuid':
                return new \Cassandra\Uuid($value);
            case 'float':
                return new \Cassandra\Float($value);
            default:
                return $value;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function execute(Statement $statement, ExecutionOptions $options = null)
    {
        return $this->connection->execute($statement, $options);
    }

    /**
     * {@inheritdoc}
     */
    public function executeAsync(Statement $statement, ExecutionOptions $options = null)
    {
        return $this->connection->executeAsync($statement, $options);
    }

    /**
     * {@inheritdoc}
     */
    public function prepare($cql, ExecutionOptions $options = null)
    {
        return $this->connection->prepare($cql, $options);
    }

    /**
     * {@inheritdoc}
     */
    public function prepareAsync($cql, ExecutionOptions $options = null)
    {
        return $this->connection->prepareAsync($cql, $options);
    }

    /**
     * {@inheritdoc}
     */
    public function close($timeout = null)
    {
        return $this->connection->close($timeout);
    }

    /**
     * {@inheritdoc}
     */
    public function closeAsync()
    {
        return $this->connection->closeAsync();
    }

    /**
     * {@inheritdoc}
     */
    public function schema()
    {
        return $this->connection->schema();
    }

    /**
     * {@inheritdoc}
     */
    protected function prepareResponse($response, CassandraEvent $event = null)
    {
        return $this->connection->prepareResponse($response, $event);
    }

    /**
     * {@inheritdoc}
     */
    protected function prepareEvent($command, array $args)
    {
        return $this->connection->prepareEvent($command, $args);
    }

    /**
     * {@inheritdoc}
     */
    protected function send($command, array $arguments)
    {
        return $this->connection->send($command, $arguments);
    }
}
