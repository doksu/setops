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

    def stream(self, records):
        if self.op == 'cardinality':
            for record in records:
                record['cardinality'] = len(set(record[self.fieldnames[0]]))
                yield record
        elif self.op == 'relation':
            for record in records:
                if set(record[self.fieldnames[0]]) == set(record[self.fieldnames[1]]):
                    relation = "equal"
                elif set(record[self.fieldnames[0]]).issubset(set(record[self.fieldnames[1]])):
                    relation = "subset"
                elif set(record[self.fieldnames[0]]).issuperset(set(record[self.fieldnames[1]])):
                    relation = "superset"
                elif len(set(record[self.fieldnames[0]]).intersection(set(record[self.fieldnames[1]]))) > 0:
                    relation = "partially disjoint"
                else:
                    relation = "fully disjoint"
                record["relation"] = relation
                yield record
        elif self.op == 'union':
            for record in records:
                record['union'] = list(set(record[self.fieldnames[0]]).union(set(record[self.fieldnames[1]])))
                yield record
        elif self.op == 'intersection':
            for record in records:
                record['intersection'] = list(set(record[self.fieldnames[0]]).intersection(set(record[self.fieldnames[1]])))
                yield record
        elif self.op == 'difference':
            for record in records:
                record['difference'] = list(set(record[self.fieldnames[0]]).difference(set(record[self.fieldnames[1]])))
                yield record
        elif self.op == 'symmetric_difference':
            for record in records:
                record['sym_diff'] = list(set(record[self.fieldnames[0]]).symmetric_difference(set(record[self.fieldnames[1]])))
                yield record
        else:
            raise Exception("Invalid op type")

dispatch(SetOpCommand, sys.argv, sys.stdin, sys.stdout, __name__)
