#!/usr/bin/env python

import sys
import json
from splunklib.searchcommands import \
    dispatch, EventingCommand, Configuration, Option, validators

from collections import defaultdict, OrderedDict

@Configuration()
class DistinctFieldsCommand(EventingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    """

    by = Option(
        doc='''
        **Syntax:** **by=***<fieldname>*
        **Description:** Name of the field to determine unique fields by''',
        require=True, validate=validators.Fieldname())

    def transform(self, events):

        # a single field makes no sense - use stats instead
        if len(self.fieldnames) < 2:
            raise Exception('Please specify at least two fields')

        event_cache = []
        tuple_counter = {}
	eventcount = 0

        for event in events:
            event_cache.append(event)
            for by_field in event[self.by]:
                for field in self.fieldnames:
                    for field_value in event[field]:
                        if (by_field, field, field_value) in tuple_counter:
                            tuple_counter[(by_field, field, field_value)] += 1
                        else:
                            tuple_counter[(by_field, field, field_value)] = 1

        for event in event_cache:
            event['distinct_fields'] = set()
            for by_field in event[self.by]:
                for field in self.fieldnames:
                    for field_value in event[field]:
                        if tuple_counter[(by_field, field, field_value)] == 1:
                            event['distinct_fields'].add(field)
            event['distinct_fields'] = list(event['distinct_fields'])
            yield event

dispatch(DistinctFieldsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
