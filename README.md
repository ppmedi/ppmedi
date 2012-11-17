ppmedi
======

By PenPals

Goal: setup histograms to compute skewness of diagnosis counts by provider

1. provider->size and provider/disease -> size
1. know % of cases at each provider have a diagnosis


In general it's

1. group by N attributes
1. normalize by M attribute
1. create histograms for each N-M value
1. compute statistics on each histogram 
    * each N-M value has a single statistic

Issue: multiple columns for a given attribute

TODO

x mapping of generalized attribute to list of attributes
1. rewrite queries to query list of attributes via union
