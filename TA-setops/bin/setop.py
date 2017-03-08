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
        **Syntax:** **op=***<cardinality|relation|union|intersection|difference|symmetric_difference|>*
        **Description:** ''',
        require=True)

    def stream(self, events):
        if self.op == 'cardinality':
            if len(self.fieldnames) != 1:
                raise Exception("One field name must be provided for cardinality.")
            for event in events:
                event['cardinality'] = len(set(event[self.fieldnames[0]]))
                yield event
        elif self.op == 'relation':
            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")
            for event in events:
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
                event["relation"] = relation
                yield event
        elif self.op == 'union':
            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")
            for event in events:
                event['union'] = list(set(event[self.fieldnames[0]]).union(set(event[self.fieldnames[1]])))
                yield event
        elif self.op == 'intersection':
            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")
            for event in events:
                event['intersection'] = list(set(event[self.fieldnames[0]]).intersection(set(event[self.fieldnames[1]])))
                yield event
        elif self.op == 'difference':
            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")
            for event in events:
                event['difference'] = list(set(event[self.fieldnames[0]]).difference(set(event[self.fieldnames[1]])))
                yield event
        elif self.op == 'symmetric_difference':
            if len(self.fieldnames) != 2:
                raise Exception("Two field names must be provided.")
            for event in events:
                event['symetric_difference'] = list(set(event[self.fieldnames[0]]).symmetric_difference(set(event[self.fieldnames[1]])))
                yield event
        else:
            raise Exception("Invalid op type")

dispatch(SetOpCommand, sys.argv, sys.stdin, sys.stdout, __name__)
