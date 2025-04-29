import pandas as pd
import re
import csv
import logging
import unicodedata
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("skills_cleaning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_skill_name(skill):
    """Clean a skill name by removing bullets, excess whitespace, etc."""
    if not isinstance(skill, str):
        return ""
    
    # Remove bullet points and similar characters from the start
    skill = re.sub(r'^[\s•\-\*\+\>\◦\‣\⁃\⦿\⦾\⁌\⁍\⧫\⧪\⧫\⸰\▪\▫\►\➢\➣\➤\·\‣\⋄\⬧\⬦\⬥\⭐\➜\➝\➞\✦\✧\❋\❊\★\☆\◆]+', '', skill)
    
    # Trim whitespace
    skill = skill.strip()
    
    return skill

def normalize_for_comparison(skill):
    """Normalize a skill name for comparison purposes."""
    if not isinstance(skill, str):
        return ""
    
    # Convert to lowercase
    normalized = skill.lower()
    
    # Remove all punctuation and special characters
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Replace multiple spaces with a single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Strip whitespace
    normalized = normalized.strip()
    
    # Normalize Unicode characters (e.g., accented characters)
    normalized = unicodedata.normalize('NFKD', normalized).encode('ASCII', 'ignore').decode('ASCII')
    
    return normalized

def is_valid_skill(skill, normalized_skill):
    """Check if a skill is valid (not just numbers or special characters)."""
    # Skip empty strings
    if not normalized_skill:
        return False
    
    # Skip skills that are only numbers
    if re.match(r'^\d+$', normalized_skill):
        return False
    
    # Skip skills that are only special characters (original skill had only punctuation)
    if len(normalized_skill) == 0 and len(skill) > 0:
        return False
    
    # Skip skills that are too short (less than 2 characters)
    if len(normalized_skill) < 2:
        return False
    
    return True

def process_skills_file(input_file="skills_data.csv", output_file="unique_skills.csv"):
    start_time = datetime.now()
    logger.info(f"Starting skills cleaning process at {start_time}")
    
    try:
        # Read the input CSV file
        logger.info(f"Reading skills from {input_file}")
        df = pd.read_csv(input_file)
        
        if 'skills' not in df.columns:
            logger.error(f"Column 'skills' not found in {input_file}")
            return
        
        # Extract skills column
        skills = df['skills'].tolist()
        logger.info(f"Found {len(skills)} skills in the input file")
        
        # Clean and process skills
        cleaned_skills = []
        normalized_map = {}  # Maps normalized form to original form
        
        for skill in skills:
            # Clean the skill name
            cleaned_skill = clean_skill_name(skill)
            
            # Normalize for comparison
            normalized_skill = normalize_for_comparison(cleaned_skill)
            
            # Check if valid
            if is_valid_skill(cleaned_skill, normalized_skill):
                # If we haven't seen this normalized form before, add it
                if normalized_skill not in normalized_map:
                    normalized_map[normalized_skill] = cleaned_skill
                    cleaned_skills.append(cleaned_skill)
        
        logger.info(f"After cleaning and deduplication, {len(cleaned_skills)} unique skills remain")
        
        # Sort skills alphabetically
        cleaned_skills.sort()
        
        # Write to output file
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['skills'])
            for skill in cleaned_skills:
                csv_writer.writerow([skill])
        
        logger.info(f"Unique skills written to {output_file}")
        logger.info(f"Process completed in {datetime.now() - start_time}")
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")

if __name__ == "__main__":
    process_skills_file()