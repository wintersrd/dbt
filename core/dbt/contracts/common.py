

def named_property(name, doc=None):
    def get_prop(self):
        return self._contents.get(name)

    def set_prop(self, value):
        self._contents[name] = value
        self.validate()

    return property(get_prop, set_prop, doc=doc)


QUOTING_CONFIG_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'identifier': {
            'type': 'boolean',
            'description': 'If True, dbt will quote identifiers',
        },
        'schema': {
            'type': 'boolean',
            'description': 'If True, dbt will quote schema names',
        },
        'database': {
            'type': 'boolean',
            'description': 'If True, dbt will quote database names',
        },
        # TODO : Add project? Where do we coalesce?
    }
}
