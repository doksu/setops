#!/usr/bin/env python

import sys
import json
from splunklib.searchcommands import \
    dispatch, EventingCommand, Configuration, Option, validators

from collections import defaultdict
from collections import OrderedDict
import tempfile
import csv

@Configuration()
class DistinctFieldsCommand(EventingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    """
    def transform(self, events):
        fieldnames = self.fieldnames

        # check a list of fields was provided
        # a single field makes no sense - use stats instead
        if len(fieldnames) < 2:
            raise Exception('Please specify at least two fields')

        workingfile = tempfile.TemporaryFile()

	eventcount = 0
        for event in events:
	    eventcount += 1
	    # create list from ordered dict then write to temp file as json string
            workingfile.write(json.dumps(list(event.viewitems()))+'\n')

	# initialise data structure
	distinctfields = [[] for x in xrange(eventcount)]

        x = {}
        for field in fieldnames:
            x[field] = defaultdict(set)

        eventnum = 0
	workingfile.seek(0)
        for line in workingfile:
	    event = OrderedDict(json.loads(line))
            for field in fieldnames:
                try:
                	for element in event[field]:
                        	# don't bother adding more than two event numbers
                        	if len(x[field][element]) < 2:
                            		x[field][element].add(eventnum)
                except KeyError:
			# there was no value for that field in this event
                	pass
	    eventnum += 1

        for field in fieldnames:
            for element in x[field]:
                if len(x[field][element]) == 1:
                    eventnumber = x[field][element].pop()
                    if field not in distinctfields[eventnumber]:
                       distinctfields[eventnumber] += [field]

        workingfile.seek(0)
        counter = 0
        for line in workingfile:
		event = OrderedDict(json.loads(line))
        	event['distinctfields'] = distinctfields[counter]
        	yield event
        	counter += 1

dispatch(DistinctFieldsCommand, sys.argv, sys.stdin, sys.stdout, __name__)
