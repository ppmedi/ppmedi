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

Results
=======
1. test on
  skewed diagnoses (in results/count.txt)
    * group by provider, UNION(dgnscd1), count
    * group by provider, UNION(dgnscd2...9), count
    * group by provider, UNION(dgnscd1...9), count
    * group by doctor, UNION(dgnscd1), count
    * group by doctor, UNION(dgnscd2...9), count
    * group by doctor, UNION(dgnscd1...9), count

  skewed costs for treatment (in results/sum.txt)
    * group by provider, UNION(dgnscd1), sum(cost)
    * group by provider, UNION(dgnscd2...9), sum(cost)
    * group by provider, UNION(dgnscd1...9), sum(cost)
    * group by doctor, UNION(dgnscd1), sum(cost)
    * group by doctor, UNION(dgnscd2...9), sum(cost)
    * group by doctor, UNION(dgnscd1...9), sum(cost)


TODOs
=======

1. Add GINI in addition to skew
1. generalize distribution getter so there's generic grouping and nomalizer
    * two group bys, aggregate, where clause (optional)
    * normalizer computed on detailed table
2. once the top k statistical results are found (call it diag X)
    1. send some example lists to Olga et al
3. Further analysis
    * show examples of the extremes
    * classify all tuples in dataset as  with diag X and without
    * filter for those tuples in base table and throw logit regression at it
4. Questions
    * What attributes are you guys even looking at?

### Tests

skewed treatments (can't find treatment column)

* group by provider, UNION(treatment1), count
* group by provider, UNION(treatment2...9), count
* group by provider, UNION(treatment1...9), count
* group by doctor, UNION(treatment1), count
* group by doctor, UNION(treatment2...9), count
* group by doctor, UNION(treatment1...9), count

for some diagnosis, is there a skewed treatment
(note: makes non-crossproduct assumption, that dgnsN is treated by treatmentN)

* group by provider, UNION(treatment1Xdgnscd1...treatmentNxdgnscdN), count
* group by doctor, UNION(treatment1Xdgnscd1...treatmentNxdgnscdN), count


Data sources
-------

* [List of ICD9 codes](https://www.section111.cms.hhs.gov/MRA/help/icd9.dx.codes.htm)
