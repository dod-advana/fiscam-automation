from pyspark.sql import DataFrame
from datetime import datetime
import pytz, pyspark.sql.functions as F
from pyspark.sql.types import StructType

class FISCAM:

  def __init__(self, fiscamdatabase: str, table: str, path: str, rawperiod: str, filelist: list, config: dict):
    # define class instance variables
    self.fiscamdatabase = fiscamdatabase
    self.database = table.split('.')[0]
    self.tablename = table.split('.')[1]
    self.path = path
    self.rawperiod = rawperiod
    self.filelist = filelist
    self.filenamefield = config['filenamefield']
    self.fieldprefix = config['fieldprefix']
    self.cdm_date = config['cdm_date']
    self.rawfields = config['rawfields']
    self.neededjoins = config['neededjoins']
    self.workingfields = config['workingfields']
    self.rules = config['rules']
    self.daterun = datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")
    self.rowcount_checks = None
    self.ingestedDF = None
    self.failedDF = None
    self.checkcolumns = []
    self.summary_counts = None

  def run_rowcount(self) -> DataFrame:
    # Generate a regex to be used in rlike to grab the preval output for the files
    fileSearch = '|'.join(self.filelist)

    # Determine if restricted preval output is needed
    preval_db = 'data_quality'
    if 'restricted' in self.database:
      preval_db += '_restricted'

    # Get the preval output
    rawCount = spark.sql(f"select * from {preval_db}.prevalidation q1 inner join (select path as temp_path, max(run_datetime) as max_datetime from {preval_db}.prevalidation group by 1) q2 on path = temp_path and run_datetime = max_datetime").drop('temp_path','max_datetime').filter(F.col('path').rlike(fileSearch))
    rawCount = rawCount.withColumns({
      'filename': F.element_at(F.split(F.col('path'), '/'), -1),
      'rawcount': 
        F.when(F.col('num_recs_total').isNull(), F.lit(0))
        .otherwise(F.col('num_recs_total'))
    }).select('filename','rawcount','overall_status','status_reason','num_recs_too_few','num_recs_too_many','trailer','header')

    # Get the ingested table for the particular files
    ingestedDF = spark.sql(f"select * from {self.database}.{self.tablename}")
    ingestedDF = ingestedDF.where(ingestedDF[self.filenamefield].isin(self.filelist))
    ingestedCount = ingestedDF.groupby(self.filenamefield).count()
    ingestedCount = ingestedCount.withColumnRenamed('count', 'ingestedcount').withColumn('ingestedcount', F.col('ingestedcount').cast('int'))

    # Join preval output to ingested counts
    joinedCount = rawCount.join(ingestedCount, rawCount['filename'] == ingestedCount[self.filenamefield], 'outer').withColumns({
      self.filenamefield:
        F.when(F.col(self.filenamefield).isNull(), F.col('filename'))
        .otherwise(F.col(self.filenamefield)),
    })
    joinedCount = joinedCount.withColumns({
      'ingestedcount':
        F.when(F.col('ingestedcount').isNull(), F.lit(0))
        .otherwise(F.col('ingestedcount'))
    })

    # Add deliverable columns
    joinedCount = joinedCount.withColumns({
      'count_compare': F.col('rawcount').cast('int') - F.col('ingestedcount').cast('int'),
      'datatable': F.lit(f"{self.database}.{self.tablename}"),
      'date_run': F.lit(self.daterun),
      'rawperiod': F.lit(self.rawperiod),
      'control_record':
        F.when(F.col('trailer').contains(F.col('rawcount')), F.col('trailer'))
        .when(F.col('header').contains(F.col('rawcount')), F.col('header'))
        .when(F.col('trailer').isNotNull(), F.col('trailer'))
        .when(F.col('header').isNotNull(), F.col('header'))
        .otherwise(F.lit('No Control Record')),
      'control_count_match':
        F.when(F.col('trailer').contains(F.col('rawcount')), F.lit('Control count matches'))
        .when(F.col('header').contains(F.col('rawcount')), F.lit('Control count matches'))
        .when(F.col('trailer').isNotNull(), F.lit('Control count DOES NOT match'))
        .when(F.col('header').isNotNull(), F.lit('Control count DOES NOT match'))
        .otherwise(F.lit('No control record'))
    })
    joinedCount = joinedCount.withColumns({
      'overall_status':
        F.when((F.col('overall_status') == 'COMPLETE') & (F.col('rawcount') == F.col('ingestedcount')), F.lit('PASSED'))
        .when((F.col('overall_status') == 'WARNING') & (F.col('rawcount') == F.col('ingestedcount')), F.lit('PASSED WITH WARNING'))
        .when((F.col('rawcount') == '0') & (F.col('ingestedcount').isNull()), F.lit('PASSED'))
        .when(F.col('rawcount') != F.col('ingestedcount'), F.lit('FAILED'))
        .otherwise(F.lit('FAILED'))
    })

    # Order the output
    joinedCount = joinedCount.select(self.filenamefield,'rawcount','ingestedcount','count_compare','overall_status','status_reason','datatable','date_run','rawperiod','control_record','control_count_match','num_recs_too_few','num_recs_too_many')

    # define the instance variable and return the rowcount df
    self.rowcount_checks = joinedCount
    return joinedCount

  def write_rowcount(self) -> str:
    # if the rowcount has not been run, output error message
    if self.rowcount_checks:
      # if rowcount has been run, attempt the append
      try:
        print(f'Beginning write to {self.fiscamdatabase}.rowcount_checks')
        self.rowcount_checks.write.format('delta').mode('append').partitionBy('datatable').option("mergeSchema", "true").saveAsTable(f'{self.fiscamdatabase}.rowcount_checks')
      except:
        return f'Failed to write to {self.fiscamdatabase}.rowcount_checks'
      else:
        return f'Completed write to {self.fiscamdatabase}.rowcount_checks'
    else:
      return 'Row count dataframe has not been run; use run_rowcount() first'

  def run_ingest(self) -> DataFrame:
    # ingest the data into it's own df using the filenames in the given path
    df = spark.sql(f"select * from {self.database}.{self.tablename}")
    df = df.where(df[self.filenamefield].isin(self.filelist))
    df = df.withColumn('rawperiod', F.lit(rawperiod))

    # define the instance variable and return the ingested df
    self.ingestedDF = df
    return df

  def run_checks(self, dropworking: bool = True) -> DataFrame:
    # if the ingested df has been created, run the checks; otherwise, print error
    if self.ingestedDF:
      # copy the ingested df into a df for the checks
      checksDF = self.ingestedDF
      workingfieldlist = []

      # if there are joins needed to be done, perform the joins
      if self.neededjoins:
        for k, v in self.neededjoins.items():
          lookup = v['lookup']
          joincolumns = v['joincolumns']
          if len(joincolumns) == 1:
            checksDF = checksDF.join(lookup, checksDF[joincolumns[0][0]] == lookup[joincolumns[0][1]], v['jointype'])
          elif len(joincolumns) == 2:
            checksDF = checksDF.join(lookup, (checksDF[joincolumns[0][0]] == lookup[joincolumns[0][1]]) & (checksDF[joincolumns[1][0]] == lookup[joincolumns[1][1]]), v['jointype'])
          elif len(joincolumns) == 3:
            checksDF = checksDF.join(lookup, (checksDF[joincolumns[0][0]] == lookup[joincolumns[0][1]]) & (checksDF[joincolumns[1][0]] == lookup[joincolumns[1][1]]) & (checksDF[joincolumns[2][0]] == lookup[joincolumns[2][1]]), v['jointype'])
          else:
            print('There must be 1-3 join column pairs')
            break
          
          workingfieldlist.extend(v['workingcolumns'])

      # if there are working fields needed, create them
      if self.workingfields:
        for k, v in self.workingfields.items():
          checksDF = checksDF.withColumn(
            k, v
          )

          workingfieldlist.append(k)

      # perform the checks to create check and failure_reason columns
      for k, v in self.rules.items():
        checksDF = checksDF.withColumn(
          f'{k}_check',
            F.when(F.trim(k) == v, F.lit('Pass'))
            .when(F.col(k).isNull() & v.isNull(), F.lit('Pass'))
            .otherwise(F.lit('Fail'))
        )
        checksDF = checksDF.withColumn(
          f'{k}_failure_reason',
            F.when(F.col(f'{k}_check') == 'Fail', F.concat(F.lit(f'{k} was not '), v))
            .otherwise(F.lit('NA'))
        )

      # if user wants to drop the working fields, drop them
      if dropworking:
        checksDF = checksDF.drop(*workingfieldlist)

      # define the instance variable and return the checksDF
      self.checksDF = checksDF
      return checksDF

    else:
      print('Ingested dataframe has not been run; use run_ingest() first')

  def run_failedrecords(self) -> DataFrame:
    # if checks df has been created, find failed records; otherwise, print error
    if self.checksDF:
      # imports for failed records optimization
      from functools import reduce as funcreduce, wraps
      from operator import and_, or_

      # define the the check columns
      self.checkcolumns = [s for s in self.checksDF.columns if s.endswith('_check') and s.startswith(self.fieldprefix)]

      # get the records that have at least one failedd check column
      failedDF = self.checksDF.where(funcreduce(or_, (F.col(c) == 'Fail' for c in self.checkcolumns)))
      failedDF = failedDF.withColumns({
        'date_run': F.lit(self.daterun),
        'rawperiod': F.lit(self.rawperiod)
      })

      # define instance variable and return the failed df
      self.failedDF = failedDF
      return failedDF

    else:
      print('Checks dataframe has not been run; use run_checks() first')

  def write_failedrecords(self) -> str:
    # if the failed df has not been created, print error
    if self.failedDF:
      # if failed df has been created, 
      try:
        print(f'Beginning write to {self.fiscamdatabase}.{self.database}_{self.tablename}_failed_records')
        self.failedDF.write.format('delta').mode('append').partitionBy('date_run').option("mergeSchema", "true").saveAsTable(f'{self.fiscamdatabase}.{self.database}_{self.tablename}_failed_records')
      except:
        return f'Failed to write to {self.fiscamdatabase}.{self.database}_{self.tablename}_failed_records'
      else:
        return f'Completed write to {self.fiscamdatabase}.{self.database}_{self.tablename}_failed_records'
    else:
      return 'Failed records dataframe has not been run; use run_failedrecords() first'

  def run_summarycounts(self) -> DataFrame:
    # if the checks df has not been created, print error
    if self.checksDF:
      # grab all of the check columns
      bigchecks = self.checksDF.select(self.checkcolumns)
      countList = []

      #convert to pandas to get number passed/failed
      pdf = bigchecks.toPandas()
      for i in self.checkcolumns:
        counts = pdf[i].value_counts()
        if 'Fail' in counts.keys():
          fail = int(counts['Fail'])
        else:
          fail = int(0)
        if 'Pass' in counts.keys():
          pas = int(counts['Pass'])
        else:
          pas = int(0)
        # add pass/fail counts to python list
        countList.append((i,fail,pas))

      # define pandas output columns and convert count list to dataframe
      columns = ['check_column', 'fail_count', 'pass_count']
      summary_counts = spark.createDataFrame(countList, columns)
      summary_counts = summary_counts.withColumns({
        'datatable': F.lit(f"{self.database}.{self.tablename}"),
        'date_run': F.lit(self.daterun),
        'rawperiod': F.lit(self.rawperiod),
        'cdm_date': F.lit(self.cdm_date)
      })

      # define instance variable and return the summary df
      self.summary_counts = summary_counts
      return summary_counts

    else:
      print('Checks dataframe has not been run; use run_checks() first')

  def write_summarycounts(self) -> str:
    # if the summary df has not been created, print error
    if self.summarycounts:
      # if the summary df has been created, attempt the append
      try:
        print(f'Beginning write to {self.fiscamdatabase}.summary_counts')
        self.summary_counts.write.format('delta').mode('append').partitionBy('datatable').option("mergeSchema", "true").saveAsTable(f'{self.fiscamdatabase}.summary_counts')
      except:
        return f'Failed to write to {self.fiscamdatabase}.summary_counts'
      else:
        return f'Completed write to {self.fiscamdatabase}.summary_counts'
    else:
      print('Summary counts dataframe has not been run; use run_summarycounts() first')

  def run_fiscam(self, rowcount_only: bool = False, dropworking: bool = True, writeresult: bool = True) -> DataFrame:

    # if row count only, run row count; otherwise, run all fiscam
    if rowcount_only:
      self.run_rowcount()
      # if user wants to write the row count, run write; otherwise, display row count
      if writeresult:
        print(self.write_rowcount())
      else:
        display(self.rowcount_checks)

      return spark.createDataFrame(data = spark.sparkContext.emptyRDD(), schema = StructType([]))

    else:
      # run all of fiscam
      self.run_rowcount()
      self.run_ingest()
      self.run_checks(dropworking = dropworking)
      failedrecords = self.run_failedrecords()
      self.run_summarycounts()
      # if user wants to write the fiscam, run writes; otherwise, display row count and summary dfs
      if writeresult:
        print(self.write_rowcount())
        print(self.write_summarycounts())
        print(self.write_failedrecords())
      else:
        display(self.rowcount_checks)
        display(self.summary_counts.orderBy('fail_count',ascending=False))

      return failedrecords
