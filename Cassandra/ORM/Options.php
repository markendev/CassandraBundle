<?php
namespace CassandraBundle\Cassandra\ORM;

/**
 * Class Options
 * @package CassandraBundle\Cassandra\ORM
 */
class Options
{
    /**
     * @var int
     */
    private $ttl;
    /**
     * @var bool
     */
    private $ifNoExist;

    /**
     * @var bool
     */
    private $exist;

    /**
     * @return int
     */
    public function getTtl()
    {
        return $this->ttl;
    }

    /**
     * @param int $ttl
     * @return Options
     */
    public function setTtl($ttl)
    {
        $this->ttl = $ttl;
        return $this;
    }

    /**
     * @return bool
     */
    public function getIfNoExist()
    {
        return $this->ifNoExist;
    }

    /**
     * @param bool $ifNoExist
     * @return Options
     */
    public function setIfNoExist($ifNoExist)
    {
        $this->ifNoExist = $ifNoExist;
        return $this;
    }

    /**
     * @return bool
     */
    public function getExist()
    {
        return $this->exist;
    }

    /**
     * @param bool $exist
     * @return Options
     */
    public function setExist( $exist)
    {
        $this->exist = $exist;
        return $this;
    }
}
