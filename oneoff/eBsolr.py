from ConfigParser import ConfigParser
from mysolr import Solr
from mysolr.compat import compat_args, parse_response
from urlparse import urljoin

import MySQLdb
import re
import requests

# from time import time

_MAXROWS = 10000
_MINCOUNT = 1

_MONTH = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
_HOURS = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
          "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]

# Sintaks for running solr multicore: java -Dsolr.solr.home=multicore -jar start.jar
# Core in example/multicore


class eBsolr:
    cursor = None

    def __init__(self, urls, config, version=4):
        self.cursor = Solr(urls, version=version)

    def update(self, documents, input_type='json', commit=False):
        self.cursor.update(documents, input_type, commit)

    def deleteById(self, tid, commit=False):
        return self.cursor.delete_by_key(tid, commit=commit)

    def deleteByQuery(self, query, commit=False):
        return self.cursor.delete_by_query(query=query, commit=commit)

    def deleteAll(self, commit=False):
        return self.cursor.delete_by_query("*:*", commit=commit)

    def getResponse(self, search, fields=None, start=0, rows=None, sort=None, fq=None):
        query = {'q': search}
        if fields:
            if isinstance(fields, basestring):
                query['fl'] = fields
            else:
                query['fl'] = ",".join(fields)
        if sort:
            query['sort'] = sort

        if fq:
            query['fq'] = fq

        # Default to 10000 rows
        limit = rows
        if rows is None:
            limit = _MAXROWS
        query['start'] = start
        query['rows'] = limit

        response = self.cursor.search(**query)
        if int(response.status) >= 400:
            raise Exception('Error Solr {}: {}'.format(response.status, response.extract_errmessage()))
        if rows is None and response.total_results > limit:
            # query['start'] = response.total_results
            query['rows'] = response.total_results
            response = self.cursor.search(**query)

        return response

    def get_language_query(self, language):
        q_temp = None
        if language is not None and language != "":
            langArray = language.split(';')
            if len(langArray) > 0:
                lang = langArray[0]
                q_temp = "language:%s" % lang
                for lang in langArray[1:]:
                    q_temp = "%s OR language:%s" % (q_temp, lang)
        return q_temp

    def getDocs(self, search, fields=None, start=0, rows=None, sort=None, fq=None):
        """search: query sintaks ex: "field:keys,field2:keys2"
           fields: field yg di ambil (list) ex: ['field', 'field2']
           start: start row
           rows: max / limit row
           sort: order rows ex: field asc, field2 desc"""
        # Get documents
        response = self.getResponse(search, fields, start, rows, sort, fq)

        return {"docs": response.documents, "count": response.total_results}

    def getFacetList(self, facets, facetField):
        ff = {}
        if not isinstance(facetField, list):
            facetField = facetField.split(",")
        for facet in facetField:
            if facet:
                ff[facet] = facets['facet_fields'][facet]

        return ff

    def getFacetPivotGeneral(self, query, facetField, pivotField, limit=None, fq=None):
        try:
            url = "{0}select?q={1}&rows=1&wt=json&indent=true&facet=true&facet.pivot={2},{3}".format(
                self.cursor.base_url, query.replace("+", "%2B"), facetField, pivotField)

            url = '{}select'.format(self.cursor.base_url)
            params = {'q': query,
                      'rows': 0,
                      'wt': 'json',
                      'indent': 'true',
                      'facet': 'true',
                      'facet.pivot': '{},{}'.format(facetField, pivotField)}

            if limit:
                params['facet.limit'] = limit
            if fq:
                params['fq'] = fq
                #                 url = "%s&facet.limit=%d" % (url, limit)
            http_response = requests.get(url, params=params)
            # print url
            #             http_response = requests.get(url)

            return http_response.json()['facet_counts']['facet_pivot']['{0},{1}'.format(facetField, pivotField)]
        except Exception, e:
            print("Error parsing facet pivot...")
            print e
        return None

    def getFacetPivot(self, query, facetField, pivotField, where=None, limit=1, fq=None):
        docsQuery = ""
        if query:
            docsQuery = "{}".format(self.queryBuilder(query))

        if where:
            docsQuery = "(%s) AND %s" % (docsQuery, where)

        return self.pivot(docsQuery, facetField, pivotField, limit)

    def pivot(self, search, facet_field, pivot_field, limit=1):
        try:
            field = "%s,%s" % (facet_field, pivot_field)
            query = {}
            query['q'] = search
            query['facet'] = 'true'
            query['facet.pivot'] = field
            query['facet.limit'] = limit
            query['facet.mincount'] = 1
            query['rows'] = 0

            queries = build_request(query)
            http_response = requests.get(urljoin(self.cursor.base_url, 'select'), params=queries)

            if http_response is not None:
                content = http_response.content
                if content is not None:
                    content = parse_response(content)
                    # print content
                    if content is not None and 'facet_counts' in content:
                        facet = content['facet_counts']
                        # print facet
                        if facet is not None and 'facet_pivot' in facet:
                            pivotList = facet['facet_pivot'][field]
                            result = {}
                            for pivot in pivotList:
                                data = {'total': 0, 'pos': 0, 'neu': 0, 'neg': 0}
                                data['total'] = pivot['count']
                                p = pivot['pivot']
                                for s in p:
                                    if str(s['value']) == '1':
                                        data['pos'] = s['count']
                                    elif str(s['value']) == '-1':
                                        data['neg'] = s['count']
                                    elif str(s['value']) == '0':
                                        data['neu'] = s['count']
                                # print data
                                result[pivot['value']] = data

                            return result
        except Exception, e:
            print "Error parsing facet pivot..."
            print e
        return None

    def getDocsWithCount(self, search, fields=None, start=0, rows=None, sort=None, fq=None):
        # Get documents
        response = self.getResponse(search, fields, start, rows, sort, fq)

        return {"docs": response.documents, "count": response.total_results}

    def getDocsFacets(self, search, facetField, fields=None, start=0, rows=0, sort=None, limit=None,
                      facetPivot=None, offset=None, minCount=None, fquery=None, fq=None, fprefix=None,
                      facetMethod=None):
        """search: query sintaks ex: "field:keys,field2:keys2"
           facetField: field yang akan di facet ex: "field"
           fields: field yg di ambil (list) ex: ['field', 'field2']
           start: start row
           rows: max / limit row
           sort: order rows ex: 'field asc, field2 desc'"""

        query = {'q': search, 'facet': 'true', 'facet.field': facetField, 'facet.mincount': _MINCOUNT}
        if fquery:
            query['facet.query'] = fquery

        if minCount:
            query['facet.mincount'] = minCount

        if facetPivot:
            query['facet.pivot'] = facetPivot

        if offset:
            query['facet.offset'] = offset

        if fields:
            if isinstance(fields, basestring):
                query['fl'] = fields
            else:
                query['fl'] = ",".join(fields)
        if sort:
            query['sort'] = sort

        if fq:
            query['fq'] = fq

        if fprefix:
            query['facet.prefix'] = fprefix

        # Default to 10000 rows
        tlimit = rows
        if rows == 0:
            tlimit = _MAXROWS
        query['start'] = start
        query['rows'] = tlimit

        # Facet Limit Default to 10000 rows
        if not limit:
            limit = _MAXROWS
        query['facet.limit'] = limit

        if facetMethod:
            query['facet.method'] = facetMethod

        response = self.cursor.search(**query)
        if rows == 0 and response.total_results > limit:
            # query['start'] = response.total_results
            query['rows'] = response.total_results
            query['facet.limit'] = response.total_results
            response = self.cursor.search(**query)
            # print response
        if limit == -1:
            query['facet.limit'] = response.total_results
        # Get documents
        documents = response.documents
        # Get facets
        tfacets = None
        facets = response.facets
        if facets:
            tfacets = self.getFacetList(facets, facetField)

        return {"docs": documents, "facets": tfacets, "result": response.total_results}

    # ================================================================================================================
    # ================================================================================================================

    def queryChecker(self, keyword, query=None, temp=None, field=''):
        key = keyword[1:]
        if not (key.endswith("*") and key.count(" ") == 0):
            key = "\"%s\"" % key
        if keyword[0] == '-':
            if query is None:
                query = "NOT %s%s" % (field, key)
            else:
                query = "%s AND NOT %s%s" % (query, field, key)
        elif keyword[0] == '&':
            if query is None:
                query = "%s%s" % (field, key)
            else:
                query = "%s AND %s%s" % (query, field, key)
        else:
            key = keyword
            if not key.endswith("*"):
                key = "\"%s\"" % key
            if query is None:
                if temp is None:
                    query = "%s%s" % (field, key)
                else:
                    query = "(%s)" % temp
            else:
                if temp is None:
                    query = "%s OR %s%s" % (query, field, key)
                else:
                    query = "%s OR (%s)" % (query, temp)
                    #        print query
        return query

    def queryBuilder(self, keywords, field=''):
        # print keywords
        if keywords.startswith('LQ:'):
            query = keywords[3:]
            query = re.sub(r' (or|and) ', lambda pat: ' {} '.format(pat.group(1).upper()), query, flags=re.I)
            query = re.sub(r'((^| |\()not) ', lambda pat: ' {} '.format(pat.group(1).upper()), query, flags=re.I)
            return query
        else:
            query = None
            if field != '':
                field += ':'
            if keywords.startswith('&'):
                keywords = keywords[1:]
            # keywords = keywords.replace(";&", " & ")
            # keywords = keywords.replace(";-", " - ")
            # print keywords
            if len(keywords.split(';')) > 1:
                # print keywords.split(';')
                for keyword in keywords.split(';'):
                    keyword = keyword.strip()
                    if len(keyword) == 0:
                        continue
                    if len(keyword.split('&')) > 1:
                        tempQuery = None
                        for key in keyword.split('&'):
                            key = key.strip()
                            if len(key) == 0:
                                continue
                            if len(key.split('-')) > 1:
                                idx = 0
                                for word in key.split('-'):
                                    word = word.strip()
                                    if len(word) == 0:
                                        continue
                                    if idx == 0:
                                        tempQuery = "%s" % self.queryChecker('&' + word, tempQuery, field=field)
                                    else:
                                        tempQuery = "%s" % self.queryChecker('-' + word, tempQuery, field=field)
                                    idx += 1
                            else:
                                tempQuery = "%s" % self.queryChecker('&' + key, tempQuery, field=field)
                        query = self.queryChecker(keyword, query, tempQuery, field=field)
                    elif len(keyword.split(' -')) > 1:
                        tempQuery = None
                        idx = 0
                        for key in keyword.split(' -'):
                            key = key.strip()
                            if len(key) == 0:
                                continue
                            if idx == 0:
                                tempQuery = "%s" % self.queryChecker(key, tempQuery, field=field)
                            else:
                                tempQuery = "%s" % self.queryChecker('-' + key, tempQuery, field=field)
                            idx += 1
                        query = self.queryChecker(keyword, query, tempQuery, field=field)
                    else:
                        query = self.queryChecker(keyword, query, field=field)
            else:
                keyword = keywords.strip()
                if len(keyword.split('&')) > 1:
                    tempQuery = None
                    for key in keyword.split('&'):
                        key = key.strip()
                        if len(key) == 0:
                            continue
                        if len(keyword.split('&')) > 1:
                            tempQuery = None
                            for key in keyword.split('&'):
                                key = key.strip()
                                if len(key) == 0:
                                    continue
                                if len(key.split('-')) > 1:
                                    idx = 0
                                    for word in key.split('-'):
                                        word = word.strip()
                                        if len(word) == 0:
                                            continue
                                        if idx == 0:
                                            tempQuery = "%s" % self.queryChecker('&' + word, tempQuery, field=field)
                                        else:
                                            tempQuery = "%s" % self.queryChecker('-' + word, tempQuery, field=field)
                                        idx += 1
                                else:
                                    tempQuery = "%s" % self.queryChecker('&' + key, tempQuery, field=field)
                    # print tempQuery
                    query = self.queryChecker(keyword, query, tempQuery, field=field)
                elif len(keyword.split(' -')) > 1:
                    print keyword
                    tempQuery = None
                    idx = 0
                    for key in keyword.split(' -'):
                        key = key.strip()
                        if len(key) == 0:
                            continue
                        if idx == 0:
                            tempQuery = "%s" % self.queryChecker(key, tempQuery, field=field)
                            # print tempQuery
                        else:
                            tempQuery = "%s" % self.queryChecker('-' + key, tempQuery, field=field)
                            # print tempQuery
                        idx += 1
                    # print tempQuery
                    query = self.queryChecker(keyword, query, tempQuery, field=field)
                    query = tempQuery
                else:
                    query = self.queryChecker(keyword, query, field=field)
            # print query
            return "(%s)" % query

    def checkQueryPrefix(self, keyword, firstQuery=False):
        query = ''
        if len(keyword) > 1:
            operand = ''
            tag = ''
            not_like = ''
            if keyword.startswith('#'):
                operand = 'OR '
                tag = keyword
            else:
                if keyword.startswith('&'):
                    operand = 'AND '
                    tag = keyword[1:]
                elif keyword.startswith('-'):
                    operand = 'AND '
                    not_like = 'NOT '
                    tag = keyword[1:]

            if firstQuery:
                operand = 'WHERE '
            query = ' ' + operand + ' tag_media ' + not_like + ' LIKE \'%' + tag + ';%\''

        return query

    def mediaCategoryParser(self, tagString):
        query = "SELECT source, category FROM clippers "
        firstQuery = True

        tagArray = tagString.split(';')
        if len(tagArray) > 1:
            for key in tagArray:
                if len(key) > 1:
                    query += self.checkQueryPrefix(key, firstQuery=firstQuery)
                firstQuery = False
        else:
            if len(tagString) > 1:
                query += self.checkQueryPrefix(tagString, firstQuery=firstQuery)

        query += ' GROUP BY source, category'
        # print query

        conf = ConfigParser()
        conf.read(self.config)

        dbhost = conf.get('database_news', 'dbhost')
        dbname = conf.get('database_news', 'dbname')
        dbuser = conf.get('database_news', 'dbuser')
        dbpwd = conf.get('database_news', 'dbpwd')

        db_conn = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpwd, db=dbname)

        cursor_km = db_conn.cursor()
        cursor_km.execute(query)
        result = cursor_km.fetchall()

        return result

    @staticmethod
    def build_media_tag_query(media_tag):
        media_tag = str(media_tag).strip().strip('&')
        if media_tag[-1] == ";":
            media_tag = media_tag[:-1]
        a = media_tag.replace(";&", " AND ").replace(";-", " OR NOT ").replace(";", " OR ").replace("-", " NOT ")
        query = a.replace('#', 'media_tag:')

        if re.search(' OR ', query):
            query = "({0})".format(query)

        m_counts = media_tag.count('#')
        n_counts = media_tag.count('-#')

        if m_counts == n_counts:
            query = " NOT ({0})".format(query.replace(' NOT', ''))

        match = re.compile(r'(.*?)([a-z:_]+[ ]?)(AND[ ]?)([a-z:_]+)([ ]?.*?)')
        # match = re.compile(r'(.*?)([a-z:_]+[ ]?)(AND[ ]?)([a-z:_]+)([ ]?.*?)', flags=re.I)
        query = match.sub(lambda pat: "{0}({1} {2} {3}){4}".format(
            pat.group(1), pat.group(2), pat.group(3), pat.group(4), pat.group(5)), query)

        return query

    @staticmethod
    def build_media_tag_query_new(media_tag):
        """ UJICOBA BUILDER MEDIA TAG BARU -AULIA- """
        query = media_tag.split(';')
        hs_or = []
        hs_and = []
        hs_not = []
        for d in query:
            if re.search('-', d):
                a = d.replace('-', ' NOT ').replace('#', 'media_tag:')
                hs_not.append(a)
            elif re.search('&', d):
                b = d.replace('&', ' AND ').replace('#', 'media_tag:')
                hs_and.append(b)
            else:
                c = d.replace('#', 'media_tag:')
                hs_or.append(c)

        data01 = " OR ".join(hs_or)
        or_count = data01.count(' OR ')
        if or_count > 1:
            data01 = "({})".format(data01)

        data02 = " OR ".join(hs_not)
        not_count = data02.count(' NOT ')
        if not_count > 1:
            data02 = " NOT ({})".format(data02.replace(" NOT ", ""))

        data03 = "".join(hs_and)

        return "{}{}{}".format(data01, data03, data02)


def build_request(query):
    """ Check solr query and put convenient format """
    assert 'q' in query
    compat_args(query)
    query['wt'] = 'json'
    return query
