<?php

namespace CassandraBundle\Cassandra\ORM;

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
     * @var string
     */
    protected $_tableName;

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
        $this->_tableName = $class->table['name'];
        $this->_em = $em;
        $this->_class = $class;
    }

    /**
     * @param string $id
     * @param string $class
     *
     * @return object
     */
    public function find($id)
    {
        return $this->_em->find($this->_tableName, $id);
    }

    /**
     * @param string $class
     *
     * @return ArrayCollection
     */
    public function findAll()
    {
        return $this->_em->findAll($this->_tableName);
    }
}
