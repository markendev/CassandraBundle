<?php

namespace CassandraBundle\Cassandra;

use Cassandra\Cluster;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Class Connection
 *
 * Connection to connect and query a cassandra cluster
 */
class Connection
{
    /**
     * @var array
     */
    protected $config;

    /**
     * @var Cluster
     */
    protected $cluster;

    /**
     * @var DefaultSession
     */
    protected $session;

    /**
     * @var string
     */
    protected $keyspace;

    /**
     * @var integer
     */
    protected $maxRetry;

    /**
     * @var EventDispatcherInterface
     */
    protected $eventDispatcher;

    /**
     * Construct the connection
     *
     * Initialize cluster and aggregate the session
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->config  = $config;
        $this->session = null;

        $this->keyspace = $config['keyspace'];
        $this->maxRetry = $config['retries']['sync_requests'];
    }

    /**
     * Set event dispatcher
     *
     * @param EventDispatcherInterface $eventDispatcher
     */
    public function setEventDispatcher(EventDispatcherInterface $eventDispatcher)
    {
        $this->eventDispatcher = $eventDispatcher;
    }

    /**
     * @return EventDispatcher
     */
    public function getEventDispatcher()
    {
        return $this->eventDispatcher;
    }

    /**
     * Set cluster
     *
     * @param Cluster $cluster
     */
    public function setCluster(Cluster $cluster)
    {
        $this->cluster = $cluster;
    }

    /**
     * @return Cluster
     */
    public function getCluster()
    {
        return $this->cluster;
    }

    /**
     * Return connection configuration
     *
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * Return keyspace to use with session
     *
     * @return string
     */
    public function getKeyspace()
    {
        return $this->keyspace;
    }

    /**
     * Return Cassandra session
     *
     * @return Session
     */
    public function getSession()
    {
        if (is_null($this->session)) {
            $this->session = $this->cluster->connect($this->getKeyspace());
        }

        return $this->session;
    }

    /**
     * Reset cassandra session
     */
    public function resetSession()
    {
        $this->session = null;
    }

    public function getMaxRetry()
    {
        return $this->maxRetry;
    }
}
