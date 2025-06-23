from typing import List, Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from .models import FeedRecord

class FeedDataNormalizer:
    @staticmethod
    def normalize_url(url_str: Optional[str]) -> Optional[str]:
        if not url_str:
            return None
        try:
            url_str = str(url_str)  # Ensure it's a string for urlparse
            
            # Handle URLs without scheme by prepending https://
            if not url_str.startswith(('http://', 'https://', 'ftp://')):
                url_str = 'https://' + url_str
            
            parsed_url = urlparse(url_str)
            
            # Remove common tracking parameters
            query_params = parse_qs(parsed_url.query)
            filtered_query_params = {
                k: v for k, v in query_params.items() 
                if not k.startswith('utm_') and k not in ['fbclid', 'gclid']
            }
            query_string = urlencode(filtered_query_params, doseq=True)

            # Reconstruct with lowercase domain
            return urlunparse((
                parsed_url.scheme,
                parsed_url.netloc.lower(),
                parsed_url.path,
                parsed_url.params,
                query_string,
                '' # No fragment
            ))
        except Exception:
            # If any error during parsing/reconstruction, return original
            return str(url_str)


    @staticmethod
    def normalize_text_field(text: Optional[str]) -> Optional[str]:
        if text is None:
            return None
        return text.strip() # Simple strip for now

    @staticmethod
    def normalize_tags(tags: List[str]) -> List[str]:
        if not tags:
            return []
        processed_tags = set()
        for tag in tags:
            if isinstance(tag, str):
                processed_tags.add(tag.strip().lower())
        return sorted(list(processed_tags))


    @classmethod
    def normalize_feed_record(cls, record: FeedRecord) -> FeedRecord:
        """
        Normalizes a single FeedRecord object.
        Modifications are made in-place to the passed record.
        """
        if record.url:
            # Pydantic's AnyUrl is an object, convert to str for normalization, then back if needed.
            # However, our normalize_url returns a string, which is fine for FeedRecord.url if it's AnyUrl.
            normalized_url_str = cls.normalize_url(str(record.url))
            if normalized_url_str:
                 # Pydantic will re-validate on assignment if type is AnyUrl
                record.url = normalized_url_str # type: ignore

        record.title = cls.normalize_text_field(record.title)
        record.description = cls.normalize_text_field(record.description)
        record.source_name = cls.normalize_text_field(record.source_name)
        record.vendor_name = cls.normalize_text_field(record.vendor_name)
        
        record.tags = cls.normalize_tags(record.tags)

        # Boolean fields (requires_payment, is_security_focused) are left as Optional[bool].
        # If they are None, they remain None. DSPy can populate them.

        # Timestamps are generally left as is, assuming they are correctly parsed initially.
        
        return record

    @classmethod
    def normalize_feed_records(cls, records: List[FeedRecord]) -> List[FeedRecord]:
        """
        Normalizes a list of FeedRecord objects.
        """
        return [cls.normalize_feed_record(record) for record in records]
