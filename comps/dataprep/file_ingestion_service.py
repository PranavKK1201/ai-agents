import redis
import requests
import os
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
"""
def create_mock_redis_data(redis_client):
    redis_client.flushall()
    logger.info("Cleared existing Redis database")
    
    """Create mock data in Redis for testing"""
    test_documents = {
        "document1": {
            "location": "/home/pranav/researchai/paper_ai/ai-agents/comps/parsers/input/2402.01968v1.pdf",
            "status": "not_processed"
        },
        "document2": {
            "location": "/home/pranav/researchai/paper_ai/ai-agents/comps/parsers/input/UniMERNet-1-2.pdf",
            "status": "processed"
        },
        "document3": {
            "location": "/home/pranav/researchai/paper_ai/ai-agents/comps/parsers/input/2402.03578v1.pdf",
            "status": "not_processed"
        }
    }
    
    for doc_name, data in test_documents.items():
        redis_client.hset(doc_name, mapping=data)
    
    return test_documents
"""
class DocumentProcessor:
    def __init__(self, redis_host='localhost', redis_port=6379):
        # Redis setup
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", redis_host),
            port=int(os.getenv("REDIS_PORT", redis_port)),
            decode_responses=True
        )
        self.api_endpoint = os.getenv("API_ENDPOINT", "http://localhost:6007/v1/dataprep")

    def process_file(self, file_path):
        """Process file using requests package"""
        file_path = Path(file_path)
        try:
            with open(file_path, 'rb') as f:
                files = {'files': (file_path.name, f)}
                response = requests.post(self.api_endpoint, files=files)
                
                if response.status_code != 200:
                    logger.error(f"Error processing file {file_path}: {response.text}")
                    return False
                
                logger.info(f"Successfully processed {file_path}")
                return True
        
        except Exception as e:
            logger.error(f"Exception while processing {file_path}: {str(e)}")
            return False

    def process_unprocessed_documents(self):
        """Process all unprocessed documents from Redis"""
        logger.info("Starting processing of unprocessed documents")
        
        # Get all documents from Redis
        all_docs = self.redis_client.keys("*")
        processed_count = 0
        
        for doc_name in all_docs:
            doc_info = self.redis_client.hgetall(doc_name)
            
            # Skip if already processed
            if doc_info.get("status") != "not_processed":
                continue
            
            file_path = doc_info.get("location")
            if not file_path:
                logger.warning(f"No location found for document: {doc_name}")
                continue
            
            logger.info(f"Processing document: {doc_name} at location: {file_path}")
            
            # For testing purposes, simulate successful processing
            # In production, uncomment the actual processing code
            success = self.process_file(file_path)
            #success = True  # Mock successful processing
            
            # Update status in Redis
            new_status = "processed" if success else "processing_failed"
            self.redis_client.hset(doc_name, "status", new_status)
            
            if success:
                processed_count += 1
            
        logger.info(f"Completed processing. Successfully processed {processed_count} documents.")

if __name__ == "__main__":
    # Initialize processor
    processor = DocumentProcessor()
    
    # Create mock data in Redis
    test_data = create_mock_redis_data(processor.redis_client)
    logger.info("Created mock Redis data:")
    for doc_name, data in test_data.items():
        logger.info(f"{doc_name}: {data}")
    
    # Process unprocessed documents
    processor.process_unprocessed_documents()
    
    # Print final status of all documents
    logger.info("\nFinal status of documents:")
    for doc_name in test_data.keys():
        status = processor.redis_client.hget(doc_name, "status")
        logger.info(f"{doc_name}: {status}")