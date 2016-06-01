<?php

namespace CassandraBundle\Cassandra\ORM;

use Doctrine\Common\Collections\ArrayCollection;

/**
 * An EntityRepository serves as a repository for entities with generic as well as
 * business specific methods for retrieving entities.
 *
 * This class is designed for inheritance and users can subclass this class to
 * write their own repositories with business-specific methods to locate entities.
 */
class EntityRepository
{
    /**
     * @var string
     */
    protected $_entityName;

    /**
     * @var EntityManager
     */
    protected $_em;

    /**
     * @var \Doctrine\ORM\Mapping\ClassMetadata
     */
    protected $_class;

    /**
     * Initializes a new <tt>EntityRepository</tt>.
     *
     * @param EntityManager         $em    The EntityManager to use.
     * @param Mapping\ClassMetadata $class The class descriptor.
     */
    public function __construct($em, Mapping\ClassMetadata $class)
    {
        $this->_entityName = $class->name;
        $this->_em         = $em;
        $this->_class      = $class;
    }

    /**
     * @param string $id
     * @param string $class
     * @return Object
     */
    public function find($id)
    {
        return $this->_em->find($this->_entityName, $id, $lockMode, $lockVersion);
        if ($id) {
            $query = sprintf("SELECT * FROM %s WHERE id = ?", $this->_class->table['name']);
            $statement = $this->_em->prepare($query);
            $arguments = new Cassandra\ExecutionOptions(['arguments' => ['id' => new Cassandra\Uuid($id)]]);
            $result = $this->_em->execute($statement, $arguments);

            $this->logger->debug('CASSANDRA: '.$query.' => ['.$id.']');

            if (($data = $result->first())) {
                return $result;
                //return $this->serializer->deserialize(json_encode($this->cleanRow($data)), $class, 'json');
            }
        }

        return null;
    }

    /**
     * @param string $class
     * @return [ArrayCollection]
     */
    public function findAll()
    {
        $query = sprintf("SELECT * FROM %s", $this->_class->table['name']);
        $statement = $this->_em->prepare($query);
        $result = $this->_em->execute($statement);

        $this->logger->debug('CASSANDRA: '.$query);

        $entities = new ArrayCollection();
        foreach ($result as $data) {
            $entities[] = $data;
        }

        return $entities;
    }
}
