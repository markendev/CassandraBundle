<?php

namespace CassandraBundle\Cassandra\ORM;

use Cassandra\BatchStatement;
use Cassandra\ExecutionOptions;
use Cassandra\Session;
use Cassandra\Statement;
use Cassandra\Type;
use CassandraBundle\Cassandra\Connection;
use CassandraBundle\Cassandra\ORM\Mapping\ClassMetadataFactoryInterface;
use CassandraBundle\Cassandra\ORM\Repository\DefaultRepositoryFactory;
use CassandraBundle\EventDispatcher\CassandraEvent;
use CassandraBundle\Cassandra\Utility\Type;
use Doctrine\Common\Collections\ArrayCollection;
use Psr\Log\LoggerInterface;

class EntityManager implements Session, EntityManagerInterface
{
    protected $connection;
    private $metadataFactory;
    private $logger;
    private $statements;
    private $repositoryFactory;
    private $schemaManager;

    const STATEMENT = 'statement';
    const ARGUMENTS = 'arguments';

    public function __construct(Connection $connection, ClassMetadataFactoryInterface $metadataFactory, LoggerInterface $logger)
    {
        $this->connection = $connection;
        $this->logger = $logger;
        $this->metadataFactory = $metadataFactory;
        $this->schemaManager = new SchemaManager($connection);
        $this->repositoryFactory = new DefaultRepositoryFactory();
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
     * Gets the repository for an entity class.
     *
     * @param string $entityName The name of the entity.
     *
     * @return \CassandraBundle\Cassandra\ORM\EntityRepository The repository class.
     */
    public function getRepository($entityName)
    {
        return $this->repositoryFactory->getRepository($this, $entityName);
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
                $argumentsString = '[';
                foreach ($statement[self::ARGUMENTS] as $argument) {
                    $value = $this->decodeColumnType($argument);
                    if (is_array($value)) {
                        $argumentsString .= sprintf('[%s]', implode(',', $value));
                    } else {
                        $argumentsString .= $value;
                    }
                    $argumentsString .= ',';
                }
                $argumentsString = substr($argumentsString, 0, -1).']';
                $this->logger->debug('CASSANDRA: '.$statement[self::STATEMENT].' => '.$argumentsString);
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
    private function encodeColumnType($type, $value = null)
    {
        if (preg_match('/set\<(.+)\>/', $type, $matches)) {
            $subType = $matches[1];
            $set = new \Cassandra\Set($this->encodeColumnType($subType));
            if ($value) {
                foreach ($value as $_value) {
                    $set->add($this->encodeColumnType($subType, $_value));
                }
            }

            return $set;
        }
        if (preg_match('/map\<(.+),\ *(.+)\>/', $type, $matches)) {
            $keyType = $matches[1];
            $valueType = $matches[2];
            $map = new \Cassandra\Map($this->encodeColumnType($keyType), $this->encodeColumnType($valueType));
            if ($value) {
                foreach ($value as $_key => $_value) {
                    $map->set($this->encodeColumnType($keyType, $_key), $this->encodeColumnType($valueType, $_value));
                }
            }

            return $map;
        }

        return Type::transformToCassandraType($type, $value);
    }

    private function decodeColumnType($columnValue)
    {
        try {
            return (string)$columnValue;
        } catch (\Exception $e) {
            // Cassandra\Map class
            if ($columnValue instanceOf \Cassandra\Map) {
                $decodedKeys = [];
                foreach ($columnValue->keys() as $key) {
                    $decodedKeys[] = $this->decodeColumnType($key);
                }
                $decodedValues = [];
                foreach ($columnValue->values() as $value) {
                    $decodedValues[] = $this->decodeColumnType($value);
                }

                return array_combine($decodedKeys, $decodedValues);
            }
            // Cassandra\Set class
            if ($columnValue instanceOf \Cassandra\Set) {
                $decodedValues = [];
                foreach ($columnValue->values() as $value) {
                    $decodedValues[] = $this->decodeColumnType($value);
                }

                return $decodedValues;
            }

            return $columnValue->values();
        }

        return $columnValue;
    }

    private function cleanRow($cassandraRow)
    {
        $cleanRow = [];
        foreach ($cassandraRow as $name => $value) {
            $cleanRow[$name] = $this->decodeColumnType($value);
        }

        return $cleanRow;
    }

    /**
     * Finds an Entity by its identifier.
     *
     * @param string       $tableName   The table name of the entity to find.
     * @param mixed        $id          The identity of the entity to find.
     *
     * @return object|null The entity instance or NULL if the entity can not be found.
     */
    public function find($tableName, $id)
    {
        if ($id) {
            $query = sprintf('SELECT * FROM %s WHERE id = ?', $tableName);
            $statement = $this->prepare($query);
            $arguments = $this->prepareArguments(['id' => new \Cassandra\Uuid($id)]);

            $this->logger->debug('CASSANDRA: '.$query.' => ['.$id.']');

            return $this->getOneOrNullResult($statement, $arguments);
        }

        return;
    }

    /**
     * Finds all entities
     *
     * @param string       $tableName   The class name of the entity to find.
     *
     * @return ArrayCollection The array of entity instance or empty array if the entity can not be found.
     */
    public function findAll($tableName)
    {
        $query = sprintf('SELECT * FROM %s', $tableName);
        $statement = $this->prepare($query);

        $this->logger->debug('CASSANDRA: '.$query);

        return $this->getResult($statement);
    }

    public function getOneOrNullResult($statement, ExecutionOptions $options = null)
    {
        $result = $this->execute($statement, $options);
        if ($result && $data = $result->first()) {
            return $this->cleanRow($data);
        }

        return;
    }

    public function getResult($statement, ExecutionOptions $options = null)
    {
        $result = $this->execute($statement);
        $entities = new ArrayCollection();
        foreach ($result as $data) {
            $entities[] = $this->cleanRow($data);
        }

        return $entities;
    }

    protected function prepareArguments($arguments)
    {
        return new ExecutionOptions([self::ARGUMENTS => $arguments]);
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
