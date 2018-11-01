#!/usr/bin/env python

from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
import sys

@Configuration()
class SetOpCommand(StreamingCommand):
    """ 

    ##Syntax


    ##Description


    ##Example


    """
    op = Option(
        doc='''
        **Syntax:** **op=***<cardinality|relation|union|intersection|difference|symmetric_difference>*
        **Description:** ''',
        require=True)

    def stream(self, events):

        if self.op == 'cardinality':

            if len(self.fieldnames) != 1:
                raise Exception("One field name must be provided for cardinality.")

            for event in events:

                try:
                    if not isinstance(event[self.fieldnames[0]], (list, tuple)):
                        event[self.fieldnames[0]] = [event[self.fieldnames[0]]]

                    event['cardinality'] = len(set(event[self.fieldnames[0]]))

                except:
                    event['cardinality'] = 0

                yield event

        elif self.op == 'relation':

            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")

            for event in events:

                try:
                    if not isinstance(event[self.fieldnames[0]], (list, tuple)):
                        event[self.fieldnames[0]] = [event[self.fieldnames[0]]]
                    if not isinstance(event[self.fieldnames[1]], (list, tuple)):
                        event[self.fieldnames[1]] = [event[self.fieldnames[1]]]

                    if set(event[self.fieldnames[0]]) == set(event[self.fieldnames[1]]):
                        relation = "equal"
                    elif set(event[self.fieldnames[0]]).issubset(set(event[self.fieldnames[1]])):
                        relation = "subset"
                    elif set(event[self.fieldnames[0]]).issuperset(set(event[self.fieldnames[1]])):
                        relation = "superset"
                    elif len(set(event[self.fieldnames[0]]).intersection(set(event[self.fieldnames[1]]))) > 0:
                        relation = "partially disjoint"
                    else:
                        relation = "fully disjoint"

                except:
                    relation = ""

                event["relation"] = relation
                yield event

        elif self.op == 'union':

            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")

            for event in events:

                try:
                    if not isinstance(event[self.fieldnames[0]], (list, tuple)):
                        event[self.fieldnames[0]] = [event[self.fieldnames[0]]]
                    if not isinstance(event[self.fieldnames[1]], (list, tuple)):
                        event[self.fieldnames[1]] = [event[self.fieldnames[1]]]

                    event['union'] = list(set(event[self.fieldnames[0]]).union(set(event[self.fieldnames[1]])))

                except:
                    if self.fieldnames[0] in event:
                        event['union'] = list(set(event[self.fieldnames[0]]))
                    elif self.fieldnames[1] in event:
                        event['union'] = list(set(event[self.fieldnames[1]]))
                    else:
                        event['union'] = []

                yield event

        elif self.op == 'intersection':

            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")

            for event in events:

                try:
                    if not isinstance(event[self.fieldnames[0]], (list, tuple)):
                        event[self.fieldnames[0]] = [event[self.fieldnames[0]]]
                    if not isinstance(event[self.fieldnames[1]], (list, tuple)):
                        event[self.fieldnames[1]] = [event[self.fieldnames[1]]]

                    event['intersection'] = list(set(event[self.fieldnames[0]]).intersection(set(event[self.fieldnames[1]])))

                except:
                    event['intersection'] = []

                yield event

        elif self.op == 'difference':

            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")

            for event in events:

                try:
                    if not isinstance(event[self.fieldnames[0]], (list, tuple)):
                        event[self.fieldnames[0]] = [event[self.fieldnames[0]]]
                    if not isinstance(event[self.fieldnames[1]], (list, tuple)):
                        event[self.fieldnames[1]] = [event[self.fieldnames[1]]]

                    event['difference'] = list(set(event[self.fieldnames[0]]).difference(set(event[self.fieldnames[1]])))

                except:
                    if self.fieldnames[0] in event:
                        event['difference'] = list(set(event[self.fieldnames[0]]))
                    else:
                        event['difference'] = []

                yield event

        elif self.op == 'symmetric_difference':

            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")

            for event in events:

                try:
                    if not isinstance(event[self.fieldnames[0]], (list, tuple)):
                        event[self.fieldnames[0]] = [event[self.fieldnames[0]]]
                    if not isinstance(event[self.fieldnames[1]], (list, tuple)):
                        event[self.fieldnames[1]] = [event[self.fieldnames[1]]]

                    event['symmetric_difference'] = list(set(event[self.fieldnames[0]]).symmetric_difference(set(event[self.fieldnames[1]])))
                except:
                    if self.fieldnames[0] in event:
                        event['symmetric_difference'] = list(set(event[self.fieldnames[0]]))
                    elif self.fieldnames[1] in event:
                        event['symmetric_difference'] = list(set(event[self.fieldnames[1]]))

                yield event

        else:
            raise Exception("Invalid op type")

dispatch(SetOpCommand, sys.argv, sys.stdin, sys.stdout, __name__)
