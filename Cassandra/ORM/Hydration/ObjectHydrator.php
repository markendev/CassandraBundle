<?php

namespace CassandraBundle\Cassandra\ORM\Hydration;

class ObjectHydrator extends AbstractHydrator
{
    public function hydrateRowData($rowData)
    {
        $className = $this->metadata->name;
        $entity = new $className();
        foreach ($this->metadata->fieldMappings as $fieldMapping) {
            $setterMethod = 'set'.ucfirst($fieldMapping['fieldName']);
            $entity->{$setterMethod}($rowData[$fieldMapping['columnName']]);
        }

        return $entity;
    }
}
