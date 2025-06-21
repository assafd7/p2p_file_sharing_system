from typing import List
import aiosqlite

class MetadataManager:
    def __init__(self, db_path, logger):
        self.db_path = db_path
        self.logger = logger

    async def get_all_metadata(self) -> List[FileMetadata]:
        """Get all file metadata"""
        self.logger.debug("Getting all file metadata")
        try:
            async with aiosqlite.connect(self.db_path) as db:
                self.logger.debug("Connected to database")
                async with db.execute("SELECT metadata FROM files") as cursor:
                    self.logger.debug("Executed query to get all files")
                    rows = await cursor.fetchall()
                    self.logger.debug(f"Retrieved {len(rows)} files from database")
                    
                    metadata_list = []
                    for row in rows:
                        try:
                            metadata = FileMetadata.from_json(row[0])
                            self.logger.debug(f"Parsed metadata for file: {metadata.name}")
                            metadata_list.append(metadata)
                        except Exception as e:
                            self.logger.error(f"Error parsing metadata: {str(e)}", exc_info=True)
                    
                    self.logger.debug(f"Successfully parsed {len(metadata_list)} metadata entries")
                    return metadata_list
        except Exception as e:
            self.logger.error(f"Error getting all metadata: {str(e)}", exc_info=True)
            return [] 