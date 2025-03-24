from sqlalchemy import create_engine, event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
import boto3
import logging
import os
import json
from urllib.parse import quote_plus

class DatabaseSession:
    """Context manager for database sessions to ensure proper cleanup"""
    def __init__(self, session_utils):
        self.session_utils = session_utils
        self.session = None
    
    def __enter__(self):
        self.session = self.session_utils.create_session()
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

class SessionUtils:
    def __init__(self, async_conn=False):
        self.async_conn = async_conn
        
        # Get AWS region
        aws_region = os.environ.get('AWS_REGION', 'us-east-1')
        secret_name = os.environ.get('DB_SECRET_NAME', 'DataPipelineAuroraClusterCredentials')
        
        # Get connection settings
        ssl_mode = os.environ.get('POSTGRES_SSL_MODE', 'verify-full')
        pool_size = int(os.environ.get('POSTGRES_POOL_SIZE', '50'))
        max_overflow = int(os.environ.get('POSTGRES_MAX_OVERFLOW', '10'))
        
        # Validate required environment variables
        required_vars = ['DB_HOSTNAME', 'DB_PORT', 'DB_NAME', 'USER']
        missing_vars = [var for var in required_vars if var not in os.environ]
        
        if missing_vars:
            logging.warning(f"Missing environment variables: {', '.join(missing_vars)}")
        
        # Get database credentials from AWS Secrets Manager
        try:
            secrets_client = boto3.client('secretsmanager', region_name=aws_region)
            secret_response = secrets_client.get_secret_value(SecretId=secret_name)
            db_creds = json.loads(secret_response['SecretString'])
            
            host = db_creds.get('host', os.environ.get('DB_HOSTNAME'))
            port = db_creds.get('port', os.environ.get('DB_PORT'))
            database = db_creds.get('dbname', os.environ.get('DB_NAME'))
            user = db_creds.get('username', os.environ.get('USER'))
            password = db_creds.get('password')
            
            # Fall back to IAM authentication if password is not in the secret
            use_iam_auth = password is None
            logging.info(f"Using credentials from Secrets Manager for database connection")
        except Exception as e:
            logging.warning(f"Failed to get database credentials from Secrets Manager: {str(e)}")
            # Fall back to environment variables
            host = os.environ.get('DB_HOSTNAME')
            port = os.environ.get('DB_PORT')
            database = os.environ.get('DB_NAME')
            user = os.environ.get('USER')
            use_iam_auth = True
            
            if not all([host, port, database, user]):
                raise ValueError("Missing required database connection parameters")
        
        # Safely encode connection parameters
        safe_user = quote_plus(user) if user else ""
        safe_host = quote_plus(host) if host else ""
        safe_database = quote_plus(database) if database else ""
        
        # Create connection string
        if use_iam_auth:
            # Use IAM authentication
            client = boto3.client("rds", region_name=aws_region)
            connection_string = f"postgresql://{safe_user}@{safe_host}:{port}/{safe_database}?sslmode={ssl_mode}&sslrootcert=/etc/pki/ca-trust/source/anchors/rds_ca.pem"
            
            # Create engine first
            if self.async_conn:
                self.engine = create_async_engine(connection_string, pool_size=pool_size, max_overflow=max_overflow)
            else:
                self.engine = create_engine(connection_string, pool_size=pool_size, max_overflow=max_overflow)
            
            # Set up event listener for IAM token
            @event.listens_for(self.engine, "do_connect")
            def provide_token(dialect, conn_rec, cargs, cparams):
                logging.info("Retrieving IAM token for database authentication")
                cparams["password"] = client.generate_db_auth_token(host, port, user)
        else:
            # Use password authentication
            safe_password = quote_plus(password) if password else ""
            connection_string = f"postgresql://{safe_user}:{safe_password}@{safe_host}:{port}/{safe_database}?sslmode={ssl_mode}&sslrootcert=/etc/pki/ca-trust/source/anchors/rds_ca.pem"
            
            # Create engine with configured pool size
            if self.async_conn:
                self.engine = create_async_engine(connection_string, pool_size=pool_size, max_overflow=max_overflow)
            else:
                self.engine = create_engine(connection_string, pool_size=pool_size, max_overflow=max_overflow)
        
        logging.info(f"Database connection initialized with pool_size={pool_size}, max_overflow={max_overflow}")

    def create_session(self):
        """Create a new database session"""
        session_factory = sessionmaker(bind=self.engine)
        Session = scoped_session(session_factory)
        return Session()

    async def create_async_session(self):
        """Create a new async database session"""
        Session = sessionmaker(bind=self.engine)
        return Session()
