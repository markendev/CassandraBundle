<?php

namespace CassandraBundle\Tests\Units\Cassandra;

use mageekguy\atoum\test;
use CassandraBundle\Cassandra\Connection as TestedClass;

class Connection extends test
{

    public function testConstruct()
    {
        $this
            ->if($testedClass = new TestedClass($this->getClusterConfig()))
            ->then
                ->string($testedClass->getKeyspace())
                    ->isEqualTo('test')
                ->array($testedClass->getConfig())
                    ->isEqualTo($this->getClusterConfig())
        ;
    }

    public function testGetSession()
    {
        $this
            ->if($testedClass = new TestedClass($this->getClusterConfig()))
            ->and($clusterMock = $this->getClusterMock())
            ->and($sessionMock = $this->getSessionMock())
            ->and($clusterMock->getMockController()->connect = $sessionMock)
            ->and($testedClass->setCluster($clusterMock))
            ->then
                ->object($testedClass->getSession())
                    ->isInstanceOf('\Cassandra\Session')
                ->object($testedClass->getSession())
                    ->isInstanceOf('\Cassandra\Session')
                ->mock($clusterMock)
                    ->call('connect')
                        ->once()
        ;
    }

    protected function getClusterConfig()
    {
        return [
            'keyspace' => 'test',
            'contact_endpoints' => ['127.0.0.1'],
            'retries' => [ 'sync_requests' => 1 ]
        ];
    }

    protected function getClusterMock()
    {
        $this->getMockGenerator()->shuntParentClassCalls();

        return new \mock\Cassandra\Cluster;
    }

    public function getSessionMock($retry = 0, $error = false)
    {
        $this->getMockGenerator()->shuntParentClassCalls();

        $session = new \mock\Cassandra\Session();
        $session->getMockController()->executeAsync = new \mock\Cassandra\Future();
        $session->getMockController()->prepareAsync = new \mock\Cassandra\Future();

        $session->getMockController()->execute = function() use (&$retry, $error) {
            if (($error && $retry <= 0) || ($retry > 0)) {
                $retry--;
                throw new \Cassandra\Exception\RuntimeException('runtime error');
            }
        };

        return $session;
    }

    public function getStatementMock()
    {
        $this->getMockGenerator()->shuntParentClassCalls();

        return new \mock\Cassandra\Statement();
    }
}
