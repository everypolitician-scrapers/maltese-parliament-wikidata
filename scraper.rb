#!/bin/env ruby
# encoding: utf-8
# frozen_string_literal: true

require 'csv'
require 'pry'
require 'scraperwiki'
require 'wikidata/fetcher'

WIKIDATA_SPARQL_URL = 'https://query.wikidata.org/sparql'.freeze

def sparql(query)
  result = RestClient.get WIKIDATA_SPARQL_URL, { accept: 'text/csv', params: { query: query } }
  CSV.parse(result, headers: true, header_converters: :symbol)
rescue RestClient::Exception => e
  raise "Wikidata query #{query} failed: #{e.message}"
end

memberships_query = <<EOQ
  SELECT DISTINCT ?item ?itemLabel ?start_date ?end_date ?partyLabel ?termID
  WHERE {
    ?item p:P39 ?statement .
    ?statement ps:P39 wd:Q19367406 ; pq:P2937 wd:Q29581248 .
    OPTIONAL { ?statement pq:P580 ?start_date }
    OPTIONAL { ?statement pq:P582 ?end_date }
    OPTIONAL { ?statement pq:P1268 ?party }
    OPTIONAL {
      ?statement pq:P2937 ?term .
      ?term p:P31 ?instance_statement .
      ?instance_statement pq:P1545 ?termID .
    }
    SERVICE wikibase:label { bd:serviceParam wikibase:language "mt,en" . }
  }
EOQ

data = sparql(memberships_query).map(&:to_h).map do |r|
  {
    id: r[:item].split('/').last,
    name: r[:itemlabel],
    start_date: r[:start_date].to_s[0..9],
    end_date: r[:end_date].to_s[0..9],
    party: r[:partylabel],
    term: r[:termid],
  }
end

ScraperWiki.sqliteexecute('DELETE FROM data') rescue nil
ScraperWiki.save_sqlite(%i(id term), data)

