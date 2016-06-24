<?php

namespace CassandraBundle\Cassandra\ORM;

class ORMException extends \Exception
{
    /**
     * @param string $mode
     *
     * @return ORMException
     */
    public static function invalidHydrationMode($mode)
    {
        return new self("'$mode' is an invalid hydration mode.");
    }
}
