<?php

namespace CassandraBundle\Cassandra\ORM\Hydration;

class ArrayHydrator extends AbstractHydrator
{
    public function hydrateRowData($rowData)
    {
        $className = $this->metadata->name;
        $arrayEntity = [];
        foreach ($this->metadata->fieldMappings as $fieldMapping) {
            if (isset($rowData[$fieldMapping['columnName']])) {
                $arrayEntity[$fieldMapping['fieldName']] = $rowData[$fieldMapping['columnName']];
            }
        }

        return $arrayEntity;
    }
}
