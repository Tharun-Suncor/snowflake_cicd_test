CREATE STAGE IF NOT EXISTS EDW.pythonstage 
	DIRECTORY = ( ENABLE = true );

PUT file://helper_class.py @EDW.pythonstage AUTO_COMPRESS=FALSE;


