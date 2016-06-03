<?php

namespace CassandraBundle;

use Symfony\Component\HttpKernel\Bundle\Bundle;

/**
 * Class CassandraBundle.
 */
class CassandraBundle extends Bundle
{
    /**
     * @return DependencyInjection\CassandraExtension|null|\Symfony\Component\DependencyInjection\Extension\ExtensionInterface
     */
    public function getContainerExtension()
    {
        return new DependencyInjection\CassandraExtension();
    }
}
