from sourceCode.pipelines.logger.loggingCode import loggingCodeClass

class checkNewFiles_c:

    def checkInitialFiles_definition(path):
    # In this function we check for the first time if there are any files arrived
        print("yes")
        # logger = loggingCodeClass().getlogger("outerNotebook")
        # try:
        #     if len(dbutils.fs.ls(path)) == 0:
        #         logger.info(f"checkInitialFiles_def() : No files arrived as of {datetime.now()} in - {path}")

        #     elif len(dbutils.fs.ls(path)) > 0:
        #         if spark.catalog.tableExists("watermark_tbl"):
        #             logger.info("watermark_tbl exists")
        #             pass
        #         else:
        #             logger.info("Creating Table watermark_tbl")
        #             pandas_df = pd.DataFrame(dbutils.fs.ls("/FileStore/tables/hellofresh"))  # Convert dbutils files list in pandas dataframe
        #             pandas_df['modificationTime'] = pandas_df['modificationTime'].apply(lambda x : datetime.fromtimestamp(int(str(x).replace('000', ''))))
        #             # Used lambda to convert type of column modificationTime from string to datetime
        #             spark.createDataFrame(pandas_df).write.saveAsTable("watermark_tbl")
        #             logger.info("watermark_tbl created")

        # except Exception as e:
        #     logger.error(f"checkInitialFiles_def() : {e}")