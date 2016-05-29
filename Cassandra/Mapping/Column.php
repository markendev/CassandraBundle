<?php

namespace CassandraBundle\Cassandra\Mapping;

use Doctrine\Common\Annotations\Annotation;

/**
 * @Annotation
 * @Target({"PROPERTY","ANNOTATION"})
 */
final class Column extends Annotation
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var string
     */
    public $type = 'text';
}
