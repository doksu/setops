#!/usr/bin/env python

import sys
from splunklib.searchcommands import \
    dispatch, ReportingCommand, Configuration, Option, validators

from collections import defaultdict

@Configuration(requires_preop=True)
class DistinctFieldsCommand(ReportingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    """
    def map(self, events):
        eventnum = 0
        eventcount = 0
        x = {}
        fieldnames = self.fieldnames

        # check a list of fields was provided
        # a single field makes no sense - use stats instead
        if len(fieldnames) < 2:
            raise Exception('Please specify at least two fields')

        for field in fieldnames:
            x[field] = defaultdict(set)

        for event in events:
            eventcount += 1
            for field in fieldnames:
                try:
                    for element in event[field]:
                        x[field][element].add(eventnum)
                except KeyError:
                    pass
	    eventnum += 1

        distinctfields = [[] for i in range(eventcount)]

        for field in x:
            for element in [field]:
                if len(x[field][element] == 1:
                    eventnumber = x[field][element].pop()
                    if field not in distinctfields[eventnumber]:
                        distinctfields[eventnumber] += [field]

        yield {self.distinctfields: distinctfields}

    def reduce(self, records):
        eventnum = 0
        for event in events:
            event['distinctfields'] = self.distinctfields[eventnum]
            eventnum += 1
            yield event

    def __init__(self):
        super(DistinctFieldsCommand, self).__init__()
        self.distinctfields = None

dispatch(DistinctFieldsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
