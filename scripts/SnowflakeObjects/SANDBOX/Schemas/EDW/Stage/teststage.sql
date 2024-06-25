CREATE STAGE IF NOT EXISTS SNOWPARK.pythonstage 
	DIRECTORY = ( ENABLE = true );

PUT 'file://~helper_class.py' @SNOWPARK.pythonstage AUTO_COMPRESS=FALSE;

