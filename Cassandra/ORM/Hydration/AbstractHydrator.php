<?php

namespace CassandraBundle\Cassandra\ORM\Hydration;

use CassandraBundle\Cassandra\ORM\Mapping\ClassMetadata;

abstract class AbstractHydrator
{
    protected $metadata;

    public function __construct(ClassMetadata $metadata)
    {
        $this->metadata = $metadata;
    }

    abstract public function hydrateRowData($rowData);
}
