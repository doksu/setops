#!/usr/bin/env python

import sys
from splunklib.searchcommands import \
    dispatch, StreamingCommand, Configuration, Option, validators

from collections import defaultdict, OrderedDict

@Configuration()
class DistinctStreamCommand(StreamingCommand):
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
        require=False, validate=validators.Fieldname())

    # produces dictionary with tuple of relevant fields as key and count as value
    # then adds 'distinctfields' to events where tuple match has a value of 1
    def stream(self, events):

        # a single field makes no sense - use stats instead
        if len(self.fieldnames) < 2:
            raise Exception('Please specify at least two fields')

        # use a fake 'by' field if none was specified
        if not self.by:
            self.by = "_distinctfields"

        tuple_counter = {}

        # first pass: cache events and produce tuple counter
        for event in events:

            # prepare the 'by' field
            if self.by == "_distinctfields":
                event["_distinctfields"] = [1]
            elif not isinstance(event[self.by], (list, tuple)):
                event[self.by] = [event[self.by]]

            # produce the tuple counter
            for by_field in event[self.by]:
                for field in self.fieldnames:
                    if field in event:
                        if not isinstance(event[field], (list, tuple)):
                            event[field] = [event[field]]
                        for field_value in event[field]:
                            if (by_field, field, field_value) in tuple_counter:
                                tuple_counter[(by_field, field, field_value)] = 2
                            else:
                                tuple_counter[(by_field, field, field_value)] = 1

            # add the distinctfields to the event
            event['distinctfields'] = set()
            for by_field in event[self.by]:
                for field in self.fieldnames:
                    if field in event:
                        for field_value in event[field]:
                            if tuple_counter[(by_field, field, field_value)] == 1:
                                event['distinctfields'].add(field)

            # clean up
            event['distinctfields'] = list(event['distinctfields'])
            if self.by == "_distinctfields":
                del event['_distinctfields']

            yield event

dispatch(DistinctStreamCommand, sys.argv, sys.stdin, sys.stdout, __name__)
