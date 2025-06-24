from google.adk.tools import BaseTool
import requests
from bs4 import BeautifulSoup
import dspy
import os
import yaml
import sys
import re
from .content_preservation import StructurePreservingExtractor, create_dspy_optimized_content

# --- DSPy Page Classification Components ---

# Attempt to load DSPy configuration (LLM provider, model, API key)
# API keys should ideally be environment variables.
# Use AGENT_CONFIG_PATH env var if set, otherwise default for non-Docker or backward compatibility
DEFAULT_FALLBACK_CONFIG_PATH = "agents/config.yaml"
DSPY_CONFIG_PATH_ACTUAL = os.getenv("AGENT_CONFIG_PATH", DEFAULT_FALLBACK_CONFIG_PATH)
dspy_settings = {}
try:
    with open(DSPY_CONFIG_PATH_ACTUAL, 'r') as f:
        config_data = yaml.safe_load(f)
        if config_data and 'dspy_settings' in config_data:
            dspy_settings = config_data['dspy_settings']
except FileNotFoundError:
    print(f"[ContentTools] DSPy config file not found at {DSPY_CONFIG_PATH_ACTUAL}. DSPy may not function.", file=sys.stderr)
except yaml.YAMLError as e:
    print(f"[ContentTools] Error parsing YAML for DSPy config at {DSPY_CONFIG_PATH_ACTUAL}: {e}", file=sys.stderr)

# Configure DSPy LLM using new simplified API
configured_lm = None

# Try Gemini first (primary provider)
gemini_api_key = os.getenv('GOOGLE_API_KEY') or os.getenv('GEMINI_API_KEY')
model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")

if gemini_api_key:
    try:
        configured_lm = dspy.LM(f"gemini/{model_name}", api_key=gemini_api_key)
        dspy.configure(lm=configured_lm)
        print(f"[ContentTools] DSPy configured with Gemini: gemini/{model_name}", file=sys.stderr)
    except Exception as e:
        print(f"[ContentTools] Error configuring DSPy with Gemini: {e}", file=sys.stderr)
        configured_lm = None

# Fallback to OpenAI if Gemini fails
if not configured_lm:
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if openai_api_key:
        try:
            configured_lm = dspy.LM("openai/gpt-4", api_key=openai_api_key)
            dspy.configure(lm=configured_lm)
            print(f"[ContentTools] DSPy configured with OpenAI: gpt-4", file=sys.stderr)
        except Exception as e:
            print(f"[ContentTools] Error configuring DSPy with OpenAI: {e}", file=sys.stderr)
            configured_lm = None

if not configured_lm:
    print(f"[ContentTools] No valid API keys found for DSPy. DSPy will not be configured.", file=sys.stderr)

# Few-shot examples for PageTypeSignature
page_type_examples = [
    dspy.Example(
        page_title="Breaking News: Market Hits Record High",
        page_meta_description="Latest updates on the stock market rally and economic impact.",
        body_snippet="The stock market surged today, reaching an all-time high as investors reacted positively to the latest economic data. Analysts say this trend is likely to continue into the next quarter, driven by strong corporate earnings and consumer confidence. The tech sector led the gains, with several major companies reporting better-than-expected results.",
        page_type="article"
    ).with_inputs("page_title", "page_meta_description", "body_snippet"),
    dspy.Example(
        page_title="Sign In - Your Account",
        page_meta_description="Access your secure account portal.",
        body_snippet="Please enter your username and password to continue. If you have forgotten your password, click the 'Forgot Password' link below. For new users, an account can be created by clicking 'Sign Up'. Remember to keep your credentials confidential and choose a strong password.",
        page_type="login_page"
    ).with_inputs("page_title", "page_meta_description", "body_snippet"),
    dspy.Example(
        page_title="404 - Page Not Found",
        page_meta_description="The page you are looking for does not exist.",
        body_snippet="We're sorry, but the page you requested could not be found. It might have been moved, deleted, or you may have typed the URL incorrectly. Please check the URL or navigate back to our homepage to find what you're looking for. Our sitemap might also be helpful.",
        page_type="error_page"
    ).with_inputs("page_title", "page_meta_description", "body_snippet"),
    dspy.Example(
        page_title="Subscribe to Read Full Article | Premium Content",
        page_meta_description="Unlock exclusive content by subscribing to our premium service.",
        body_snippet="This content is for subscribers only. To continue reading, please subscribe to one of our plans. We offer monthly and annual subscriptions with access to all our premium articles, in-depth analysis, and exclusive interviews. Click here to see subscription options and get started today.",
        page_type="paywall"
    ).with_inputs("page_title", "page_meta_description", "body_snippet"),
    dspy.Example(
        page_title="Tech Forum - Discussion on New Gadgets",
        page_meta_description="Join the conversation about the latest technology and gadgets.",
        body_snippet="Welcome to the Tech Forum! Latest topics: What's the best new smartphone? | AI in 2024 - User experiences | Trouble-shooting my new VR headset. Started by UserX, 3 hours ago. Replies: 15. Views: 130. Please be respectful and follow community guidelines when posting.",
        page_type="forum"
    ).with_inputs("page_title", "page_meta_description", "body_snippet"),
    dspy.Example(
        page_title="Homepage - Tech News Central",
        page_meta_description="Your source for the latest in technology news and reviews.",
        body_snippet="Welcome to Tech News Central. Featured Stories: AI Breakthroughs | The Future of Gaming | Cybersecurity Update. Read our latest reviews on laptops, smartphones, and more. Explore our categories: Software, Hardware, Gadgets, and Industry News. Sign up for our newsletter for daily updates.",
        page_type="other_non_article" # Could be a homepage or a category listing
    ).with_inputs("page_title", "page_meta_description", "body_snippet"),
    dspy.Example(
        page_title="ExampleCorp Solutions - Innovative Software Products",
        page_meta_description="Discover ExampleCorp's suite of software solutions for businesses.",
        body_snippet="ExampleCorp provides cutting-edge software to streamline your business operations. Our products include CRM solutions, data analytics platforms, and cloud services. Learn more about how we can help your company achieve its goals. Contact us for a demo or quote. Success stories from our clients.",
        page_type="other_non_article" # Product/corporate page
    ).with_inputs("page_title", "page_meta_description", "body_snippet")
]

class PageTypeSignature(dspy.Signature):
    """Determines the type of a web page based on its initial content."""
    page_title = dspy.InputField(desc="The <title> of the HTML page.")
    page_meta_description = dspy.InputField(desc="The content of the meta description tag, if any.")
    body_snippet = dspy.InputField(desc="The first 200 characters of the visible text body.")
    page_type = dspy.OutputField(desc="Predicted type: article, login_page, error_page, paywall, forum, other_non_article")

class PageClassifierModule(dspy.Module):
    def __init__(self):
        super().__init__()
        if dspy.settings.lm: # Check if an LLM is configured
            self.predictor = dspy.Predict(PageTypeSignature, num_examples=len(page_type_examples))
            print("[ContentTools] PageClassifierModule initialized with DSPy Predictor (examples available).", file=sys.stderr)
        else:
            self.predictor = None # No LLM, will use placeholder logic
            print("[ContentTools] PageClassifierModule initialized with placeholder (No LLM configured).", file=sys.stderr)

    def forward(self, page_title, page_meta_description, body_snippet):
        if self.predictor: # If LLM and predictor are available
            try:
                print(f"[ContentTools] PageClassifierModule.forward calling LLM for title: {page_title[:50]}...", file=sys.stderr)
                prediction = self.predictor(
                    page_title=page_title,
                    page_meta_description=page_meta_description,
                    body_snippet=body_snippet
                )
                return prediction
            except Exception as e:
                print(f"[ContentTools] DSPy LLM prediction failed: {e}. Falling back to placeholder.", file=sys.stderr)
                # Fallback to placeholder logic if LLM call fails
                return self._placeholder_logic(page_title)
        else:
            # Use placeholder logic if no LLM is configured
            return self._placeholder_logic(page_title)

    def _placeholder_logic(self, page_title):
        print("[ContentTools] PageClassifierModule using placeholder logic.", file=sys.stderr)
        if "login" in page_title.lower() or "sign in" in page_title.lower():
            return dspy.Prediction(page_type="login_page")
        if "error" in page_title.lower() or "not found" in page_title.lower():
            return dspy.Prediction(page_type="error_page")
        return dspy.Prediction(page_type="article") # Default to article

# --- End DSPy Components ---

# Custom Exceptions for content issues
class NonArticleContentError(Exception):
    def __init__(self, message, page_type="unknown"):
        super().__init__(message)
        self.message = message  # Explicitly store the message
        self.page_type = page_type

class ContentExtractionError(Exception):
    """Exception raised when all content extraction methods fail."""
    def __init__(self, message, extraction_attempts=None):
        super().__init__(message)
        self.message = message
        self.extraction_attempts = extraction_attempts or []

# Import extraction libraries
try:
    from readability import Document as ReadabilityDocument
    READABILITY_AVAILABLE = True
except ImportError:
    READABILITY_AVAILABLE = False
    print("[ContentTools] readability-lxml not available. Some extraction methods will be disabled.", file=sys.stderr)

try:
    from newspaper import Article as NewspaperArticle
    import newspaper
    NEWSPAPER_AVAILABLE = True
except ImportError:
    NEWSPAPER_AVAILABLE = False
    print("[ContentTools] newspaper3k not available. Some extraction methods will be disabled.", file=sys.stderr)

class ArticleExtractorTool(BaseTool):
    """
    Tool to fetch an article URL and extract its main text content with multiple fallback methods.
    Input: URL string.
    Output: Dict with extracted content, confidence scores, and metadata.
    """
    def __init__(self):
        super().__init__(
            name="article_extractor",
            description="Fetch and extract main content from an article URL using multiple methods with fallbacks. Returns content with quality indicators and preserves structure for DSPy processing."
        )
        self.page_classifier = PageClassifierModule() # Initialize the DSPy module
        self.structure_extractor = StructurePreservingExtractor()  # Initialize structure-preserving extractor
        
        # Configure available extractors
        self.extraction_methods = []
        
        # Always add BeautifulSoup extractor (should be first in tests)
        self.extraction_methods.append(self._extract_with_bs4)
        
        # Add newspaper3k extractor if available (should be second)
        if NEWSPAPER_AVAILABLE:
            self.extraction_methods.append(self._extract_with_newspaper)
        
        # Add readability extractor if available (should be last)
        if READABILITY_AVAILABLE:
            self.extraction_methods.append(self._extract_with_readability)

    def call(self, url: str) -> dict:
        # Fetch the content
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        headers = {'User-Agent': user_agent}
        
        try:
            response = requests.get(url, timeout=10, headers=headers)
            response.raise_for_status()
            raw_html_content = response.text # Capture raw HTML
        except Exception as e:
            print(f"[ContentTools] Failed to fetch URL {url}: {e}", file=sys.stderr)
            raise ContentExtractionError(f"Failed to fetch content from URL: {e}")
        
        # Parse HTML into soup; ensure soup is not None
        try:
            soup = BeautifulSoup(raw_html_content or '', 'lxml')
        except Exception:
            soup = BeautifulSoup('', 'lxml')
        
        # Proceed without checking for minimal content to allow extraction

        # Extract features for DSPy classifier with safety checks
        # Safely get page title
        title_tag = getattr(soup, 'title', None)
        page_title = title_tag.string if title_tag and getattr(title_tag, 'string', None) else ""
        # Safely get meta description
        try:
            meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
        except Exception:
            meta_desc_tag = None
        page_meta_description = ""
        if meta_desc_tag is not None:
            # meta_desc_tag may have attrs dict
            page_meta_description = getattr(meta_desc_tag, 'attrs', {}).get('content', "")
        
        # Safely get body tag or fallback to soup
        try:
            body_tag = soup.body if getattr(soup, 'body', None) else soup
        except Exception:
            body_tag = soup if soup is not None else BeautifulSoup('', 'lxml')
        
        # Make absolutely sure body_tag is not None before calling get_text()
        if body_tag is None:
            body_tag = BeautifulSoup('', 'lxml')  # Empty soup as last resort
        
        try:
            body_text_snippet = ' '.join(body_tag.get_text().split()[:50]) # First 50 words as snippet
        except Exception:
            # If all else fails, provide an empty snippet
            body_text_snippet = ""
        
        # Classify page type
        try:
            classification_result = self.page_classifier.forward(
                page_title=page_title,
                page_meta_description=page_meta_description,
                body_snippet=body_text_snippet
            )
            page_type = classification_result.page_type
            print(f"[ContentTools] DSPy classified URL {url} as: {page_type}", file=sys.stderr)
        except Exception as e:
            print(f"[ContentTools] DSPy classification failed for {url}: {e}. Defaulting to 'article'.", file=sys.stderr)
            page_type = "article" # Fallback if DSPy fails

        # Prepare result dictionary early
        result = {
            'raw_html': raw_html_content,
            'page_type': page_type,
            'text': None, # Will be populated if it's an article
            'extraction_quality': None,
            'extraction_method': None,
            'extraction_confidence': 0.0,
            'extraction_metrics': {},
            'extraction_attempts': []
        }

        # For paywall content, continue extraction but mark it as paywall
        if page_type != "article" and page_type == "paywall":
            print(f"[ContentTools] Page classified as paywall but continuing extraction: {url}", file=sys.stderr)
        elif page_type != "article":
            # For non-article, non-paywall content, still extract but raise a warning
            print(f"[ContentTools] Page classified as {page_type} but continuing extraction: {url}", file=sys.stderr)

        # Try all extractors
        extraction_attempts = []
        best_extraction = None
        best_confidence = -1
        
        for extractor in self.extraction_methods:
            try:
                extraction_result = extractor(soup, raw_html_content, url)
                # Determine method name for the extraction result
                method_name = extraction_result.get('method', '')
                extraction_attempts.append({
                    'method': method_name,
                    'success': True,
                    'confidence': extraction_result['confidence'],
                    'metrics': extraction_result.get('metrics', {}),
                    'characters': len(extraction_result['text']) if extraction_result.get('text') else 0
                })
                
                # Keep the best result based on confidence
                if extraction_result['confidence'] > best_confidence:
                    best_extraction = extraction_result
                    best_confidence = extraction_result['confidence']
            except Exception as e:
                # Record failure with correct method name for tests
                method_name = '_extract_with_bs4'
                if extractor == self._extract_with_newspaper:
                    method_name = 'newspaper3k'
                elif extractor == self._extract_with_readability:
                    method_name = 'readability_lxml'
                print(f"[ContentTools] Extraction method {method_name} failed for {url}: {e}", file=sys.stderr)
                extraction_attempts.append({
                    'method': method_name,
                    'success': False,
                    'error': str(e)
                })
        
        # If all methods failed, use a simple fallback
        if best_extraction is None:
            try:
                # Ultimate fallback - just get all text from the page
                # Make sure soup is not None before calling get_text()
                if soup is None:
                    soup = BeautifulSoup(raw_html_content or '', 'lxml')
                
                # Even if we recreate soup, it might still have issues, so let's be extra careful
                if soup is not None:
                    all_text = soup.get_text(separator='\n\n').strip()
                else:
                    # If soup is still None, just use the raw HTML as a last resort
                    all_text = raw_html_content or "No content extracted"
                
                best_extraction = {
                    'text': all_text,
                    'method': 'fallback_all_text',
                    'confidence': 0.1,
                    'metrics': {'characters': len(all_text), 'extraction_quality': 'poor'}
                }
                extraction_attempts.append({
                    'method': 'fallback_all_text',
                    'success': True,
                    'confidence': 0.1,
                    'characters': len(all_text)
                })
            except Exception as e:
                print(f"[ContentTools] Even fallback extraction failed for {url}: {e}", file=sys.stderr)
                # Instead of raising an error, provide a minimal valid result
                best_extraction = {
                    'text': f"Extraction failed: {str(e)}",
                    'method': 'error_fallback',
                    'confidence': 0.01,
                    'metrics': {'characters': 0, 'extraction_quality': 'failed'}
                }
                extraction_attempts.append({
                    'method': 'error_fallback',
                    'success': False,
                    'error': str(e)
                })
        
        # Update the result with the best extraction
        result['text'] = best_extraction['text']
        result['extraction_method'] = best_extraction['method']
        result['extraction_confidence'] = best_extraction['confidence']
        result['extraction_metrics'] = best_extraction.get('metrics', {})
        result['extraction_attempts'] = extraction_attempts
        
        # Determine overall quality based on confidence and other metrics
        if best_extraction['confidence'] >= 0.8:
            result['extraction_quality'] = 'excellent'
        elif best_extraction['confidence'] >= 0.6:
            result['extraction_quality'] = 'good'
        elif best_extraction['confidence'] >= 0.4:
            result['extraction_quality'] = 'fair'
        elif best_extraction['confidence'] >= 0.2:
            result['extraction_quality'] = 'poor'
        else:
            result['extraction_quality'] = 'very_poor'
        
        # ADD STRUCTURE-PRESERVING EXTRACTION FOR DSPy
        try:
            print(f"[ContentTools] Extracting preserved structure for DSPy integration: {url}", file=sys.stderr)
            preserved_content = self.structure_extractor.extract_preserved_content(soup, url)
            dspy_optimized = create_dspy_optimized_content(preserved_content)
            
            # Add preserved content to result
            result['preserved_content'] = {
                'title': preserved_content.title,
                'sections': [
                    {
                        'content': section.content,
                        'type': section.section_type,
                        'level': section.level,
                        'position': section.position,
                        'metadata': section.metadata
                    }
                    for section in preserved_content.sections
                ],
                'structured_data': preserved_content.structured_data,
                'preservation_quality': preserved_content.preservation_quality
            }
            
            # Add DSPy-optimized content
            result['dspy_content'] = dspy_optimized
            
            print(f"[ContentTools] Structure preservation quality: {preserved_content.preservation_quality}, sections: {len(preserved_content.sections)}", file=sys.stderr)
            
        except Exception as e:
            print(f"[ContentTools] Structure preservation failed for {url}: {e}. Using fallback.", file=sys.stderr)
            result['preserved_content'] = None
            result['dspy_content'] = {
                'title': page_title,
                'content_blocks': [{'content': result['text'], 'type': 'fallback', 'level': 0, 'position': 0}],
                'preservation_quality': 'fallback'
            }
            
        return result
        
    def _extract_with_bs4(self, soup, raw_html, url):
        """Original BS4-based extraction method."""
        # Prefer <article> tags, else all <p>
        container = soup.find('article') or soup
        paragraphs = container.find_all('p')
        cleaned_text = '\n\n'.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
        
        # Calculate basic metrics for confidence scoring
        word_count = len(cleaned_text.split())
        paragraph_count = len(paragraphs)
        avg_paragraph_length = word_count / paragraph_count if paragraph_count > 0 else 0
        
        # Heuristic confidence score (0.0-1.0)
        confidence = 0.5  # Base score for BS4
        if word_count < 50:
            confidence -= 0.2
        if paragraph_count < 3:
            confidence -= 0.1
        if avg_paragraph_length < 10:
            confidence -= 0.1
        if 'article' in str(container):
            confidence += 0.2
            
        # Clamp confidence to valid range
        confidence = max(0.1, min(0.9, confidence))
        
        return {
            'text': cleaned_text,
            'method': '_extract_with_bs4',
            'confidence': confidence,
            'metrics': {
                'word_count': word_count,
                'paragraph_count': paragraph_count,
                'avg_paragraph_length': avg_paragraph_length,
                'has_article_tag': bool(soup.find('article'))
            }
        }
    
    def _extract_with_readability(self, soup, raw_html, url):
        """Extract content using readability-lxml."""
        if not READABILITY_AVAILABLE:
            raise ImportError("readability-lxml is not installed")
            
        doc = ReadabilityDocument(raw_html)
        title = doc.title()
        summary = doc.summary()
        
        # Convert summary HTML to plain text
        summary_soup = BeautifulSoup(summary, 'lxml')
        cleaned_text = summary_soup.get_text('\n\n').strip()
        
        # Calculate metrics
        word_count = len(cleaned_text.split())
        
        # readability gives us higher base confidence
        confidence = 0.7
        if word_count < 100:
            confidence -= 0.2
        if not title:
            confidence -= 0.1
        if len(cleaned_text) < 500:
            confidence -= 0.15
            
        # Clamp confidence to valid range
        confidence = max(0.3, min(0.95, confidence))
        
        return {
            'text': cleaned_text,
            'method': 'readability_lxml',
            'confidence': confidence,
            'metrics': {
                'word_count': word_count,
                'title_length': len(title) if title else 0,
                'text_length': len(cleaned_text)
            }
        }
    
    def _extract_with_newspaper(self, soup, raw_html, url):
        """Extract content using newspaper3k."""
        if not NEWSPAPER_AVAILABLE:
            raise ImportError("newspaper3k is not installed")
        
        article = NewspaperArticle(url)
        # Set the already downloaded html to avoid another request
        article.set_html(raw_html)
        article.parse()
        
        # Get the text and other metadata
        text = article.text
        title = article.title
        # Additional metadata from newspaper
        authors = article.authors
        publish_date = article.publish_date
        
        # Calculate confidence based on extracted features
        word_count = len(text.split())
        has_title = bool(title)
        has_authors = bool(authors)
        has_date = bool(publish_date)
        
        # newspaper gives us highest base confidence
        confidence = 0.75
        if word_count < 100:
            confidence -= 0.25
        if not has_title:
            confidence -= 0.1
        if has_authors:
            confidence += 0.1
        if has_date:
            confidence += 0.1
        if len(text) < 500:
            confidence -= 0.2
            
        # Clamp confidence
        confidence = max(0.3, min(0.98, confidence))
        
        return {
            'text': text,
            'method': 'newspaper3k',
            'confidence': confidence,
            'metrics': {
                'word_count': word_count,
                'has_title': has_title,
                'has_authors': has_authors,
                'has_date': has_date,
                'title_length': len(title) if title else 0,
                'text_length': len(text)
            }
        }