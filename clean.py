from pymongo import MongoClient
import re
import logging
from datetime import datetime
import time
from typing import List, Dict
import os

# Set up logging
def setup_logger():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Create a logger
    logger = logging.getLogger('language_cleanup')
    logger.setLevel(logging.INFO)
    
    # Create file handler with timestamp in filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    fh = logging.FileHandler(f'logs/language_cleanup_{timestamp}.log')
    fh.setLevel(logging.INFO)
    
    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger

def normalize_proficiency(proficiency):
    """Normalize proficiency levels to standard format"""
    if not proficiency:
        return None
        
    # Convert to lowercase for comparison
    prof = proficiency.lower().strip()
    
    # Handle A1-A5 format
    a_level_match = re.match(r'^a[1-5]$', prof)
    if a_level_match:
        return prof.upper()
    
    # Standardize various spellings and formats
    proficiency_mapping = {
        # Beginner variations
        'beginner': 'Beginner',
        'basic': 'Beginner',
        'elementary': 'Beginner',
        'novice': 'Beginner',
        'fundamental': 'Beginner',
        
        # Intermediate variations
        'intermediate': 'Intermediate',
        'medium': 'Intermediate',
        'mid': 'Intermediate',
        'mid level': 'Intermediate',
        'mid-level': 'Intermediate',
        
        # Advanced variations
        'advanced': 'Advanced',
        'advance': 'Advanced',
        'expert': 'Advanced',
        'fluent': 'Advanced',
        'proficient': 'Advanced',
        'professional': 'Advanced'
    }
    
    return proficiency_mapping.get(prof)

def extract_proficiency_from_name(language_str):
    """Extract proficiency level from language name and return cleaned name and proficiency"""
    # Regular expressions for matching proficiency levels
    proficiency_patterns = [
        r'\((.*?)\)',  # Match anything in parentheses
        r'- *(beginner|basic|intermediate|advance[d]?|a[1-5])',  # Match after hyphen
        r', *(beginner|basic|intermediate|advance[d]?|a[1-5])',  # Match after comma
        r' (beginner|basic|intermediate|advance[d]?|a[1-5])$'    # Match at end
    ]
    
    language_str = language_str.lower().strip()
    extracted_proficiency = None
    
    # Try each pattern
    for pattern in proficiency_patterns:
        match = re.search(pattern, language_str, re.IGNORECASE)
        if match:
            # Extract the proficiency and remove it from the language name
            extracted_proficiency = match.group(1)
            language_str = re.sub(pattern, '', language_str, flags=re.IGNORECASE)
            break
    
    return language_str.strip(), normalize_proficiency(extracted_proficiency)

def process_batch(batch: List[Dict], collection, logger) -> tuple:
    """Process a batch of documents and return update counts"""
    batch_updates = 0
    batch_errors = 0
    
    for doc in batch:
        try:
            if 'languages' not in doc:
                continue
            
            current_languages = doc['languages']
            cleaned_languages = []
            
            for lang_obj in current_languages:
                if 'language' not in lang_obj:
                    continue
                    
                lang_str = lang_obj['language']
                
                if not lang_str:
                    continue
                
                initial_split = [l.strip() for l in lang_str.split(',')]
                
                final_languages = []
                for item in initial_split:
                    and_split = [l.strip() for l in re.split(r'\s+and\s+', item)]
                    final_languages.extend(and_split)
                
                for lang in final_languages:
                    if lang:
                        clean_lang, extracted_prof = extract_proficiency_from_name(lang)
                        
                        final_proficiency = extracted_prof
                        if not final_proficiency:
                            final_proficiency = normalize_proficiency(lang_obj.get('proficiency'))
                        
                        if clean_lang:
                            cleaned_languages.append({
                                'language': clean_lang.strip(),
                                'proficiency': final_proficiency
                            })
            
            if cleaned_languages != current_languages:
                collection.update_one(
                    {'_id': doc['_id']},
                    {'$set': {'languages': cleaned_languages}}
                )
                batch_updates += 1
                logger.debug(f"Updated document {doc['_id']}")
                
        except Exception as e:
            batch_errors += 1
            logger.error(f"Error processing document {doc.get('_id')}: {str(e)}")
            continue
            
    return batch_updates, batch_errors

def clean_languages(db_uri: str, db_name: str, collection_name: str, batch_size: int = 100):
    """Main function to clean language fields with batch processing"""
    logger = setup_logger()
    start_time = time.time()
    
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = MongoClient(db_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        # Get total count for progress tracking
        total_docs = collection.count_documents({"source": "coresignal"})
        logger.info(f"Found {total_docs} documents to process")
        
        # Initialize counters
        total_updates = 0
        total_errors = 0
        processed = 0
        
        # Process in batches
        cursor = collection.find({"source": "coresignal"}, batch_size=batch_size)
        current_batch = []
        
        for doc in cursor:
            current_batch.append(doc)
            
            if len(current_batch) == batch_size:
                logger.info(f"Processing batch of {batch_size} documents...")
                updates, errors = process_batch(current_batch, collection, logger)
                total_updates += updates
                total_errors += errors
                processed += len(current_batch)
                
                # Log progress
                progress = (processed / total_docs) * 100
                logger.info(f"Progress: {progress:.2f}% ({processed}/{total_docs})")
                logger.info(f"Current stats - Updates: {total_updates}, Errors: {total_errors}")
                
                current_batch = []
        
        # Process remaining documents
        if current_batch:
            logger.info(f"Processing final batch of {len(current_batch)} documents...")
            updates, errors = process_batch(current_batch, collection, logger)
            total_updates += updates
            total_errors += errors
        
        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"Execution completed in {execution_time:.2f} seconds")
        logger.info(f"Final results - Total Updates: {total_updates}, Total Errors: {total_errors}")
        
        return {
            'total_updates': total_updates,
            'total_errors': total_errors,
            'execution_time': execution_time,
            'status': 'completed'
        }
        
    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}")
        return {
            'status': 'failed',
            'error': str(e)
        }
    finally:
        client.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    # Connection details
    DB_URI = "CONNECTION_STRING"
    DB_NAME = "DEV_STUDENT" #PROD_STUDENT
    COLLECTION_NAME = "students"
    BATCH_SIZE = 100  # Adjust based on your needs
    
    result = clean_languages(DB_URI, DB_NAME, COLLECTION_NAME, BATCH_SIZE)
    print(result)
