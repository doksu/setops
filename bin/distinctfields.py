#!/usr/bin/env python

import sys
from splunklib.searchcommands import \
    dispatch, EventingCommand, Configuration, Option, validators

from collections import defaultdict

@Configuration()
class DistinctCommand(EventingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    """
    def transform(self, events):
        eventnum = 0
        x = {}
        fieldnames = self.fieldnames

        # check a list of fields was provided
        # a single field makes no sense - use stats instead
        if len(fieldnames) < 2:
            raise Exception('Please specify at least two fields')

        workingset = []
        for event in events:
            workingset.append(event)

        for field in fieldnames:
            x[field] = defaultdict(set)

        for event in workingset:
            for field in fieldnames:
                try:
                    for element in event[field]:
                        x[field][element].add(eventnum)
                except KeyError:
                    pass
            event['distinctfields'] = []
	    eventnum += 1

        for field in fieldnames:
            for element in x[field]:
                if len(x[field][element]) == 1:
                    workingset[x[field][element].pop()]['distinctfields'] += [field]

        for event in workingset:
            yield event

dispatch(DistinctCommand, sys.argv, sys.stdin, sys.stdout, __name__)
