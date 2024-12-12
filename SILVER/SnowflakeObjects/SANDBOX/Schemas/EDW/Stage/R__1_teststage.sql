CREATE STAGE IF NOT EXISTS EDW.pythonstage 
	DIRECTORY = ( ENABLE = true );

PUT './helper_class.py' @EDW.pythonstage AUTO_COMPRESS=FALSE;


