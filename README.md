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

TODOs
=======

1. test on
    * group by provider, diagnoses, count
    * group by provider, diagnoses, sum(cost)

1. generalize distribution getter so there's generic grouping and nomalizer
    * two group bys, aggregate, where clause (optional)
    * normalizer computed on detailed table
2. once the top k statistical results are found (call it diag X)
    1. send some example lists to Olga et al
3. Further analysis
    * show examples of the extremes
    * classify all tuples in dataset as  with diag X and without
    * filter for those tuples in base table and throw logit regression at it

Data sources
-------

* [List of ICD9 codes](https://www.section111.cms.hhs.gov/MRA/help/icd9.dx.codes.htm)
