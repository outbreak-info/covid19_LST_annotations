## COVID-19 Literature Surveillance Team parser for Outbreak.info
[![DOI](https://zenodo.org/badge/322128248.svg)](https://zenodo.org/badge/latestdoi/322128248)

This is the parser for the COVID-19 Literature Surveillance Team reports.  The COVID-19 Literature Surveillance Team is a group of MDs, PhDs, Medical Students and Residents who summarize and evaluate recent publications on COVID-19. They use the Bottom Line Up Front (BLUF) standard for their summaries and the 2011 Oxford level of evidence rubric for evaluating level of evidence.

This parser creates value added information to be appended to existing publication metadata records.

Note that the COVID-19 LST team provides two different types of data, which will be parsed on different schedules which is why the initial script has been split into to two.

This repo is for the annotations provided by the COVID-19 Literature Surveillance team to be added to individual pmids from lit covid.  The update of the annotations file should be run on a weekly basis (as we will recieve weekly dumps), but the inclusion of the annotations to pmid metadata records should be run daily (as they will be erased after each litcovid update)

Update 12/2021 - As of July 2021, COVID-19 LST will no longer provide updates. Hence, this repo will primarily serve as a home for existing parsed data.
