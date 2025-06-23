"""
Intelligent Content Analyzer for Enhanced Feed Enrichment

This module provides sophisticated content analysis that goes beyond simple keyword matching
to understand context, patterns, and implicit security references.
"""

import re
import logging
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from collections import Counter
import statistics

logger = logging.getLogger(__name__)


@dataclass
class ContentAnalysisResult:
    """Result of comprehensive content analysis"""
    content_type: str
    confidence: float
    security_indicators: Dict[str, float]
    context_clues: List[str]
    detected_entities: Dict[str, List[str]]
    recommendation: str
    analysis_depth: str


class IntelligentContentAnalyzer:
    """
    Sophisticated content analyzer that understands context and implicit references
    to security threats, even when not explicitly mentioned in titles.
    """
    
    def __init__(self):
        # Expanded security indicators with context patterns
        self.security_patterns = {
            # Direct threat indicators
            'threat_actors': {
                'keywords': [
                    'apt', 'apt28', 'apt29', 'lazarus', 'fin7', 'carbanak', 'turla',
                    'cozy bear', 'fancy bear', 'equation group', 'darkhydrus',
                    'threat actor', 'threat group', 'campaign', 'operation'
                ],
                'weight': 0.9
            },
            
            # Attack patterns and techniques
            'attack_techniques': {
                'keywords': [
                    'phishing', 'spear phishing', 'malware', 'ransomware', 'trojan',
                    'backdoor', 'rootkit', 'exploit', 'zero-day', '0-day', 'vulnerability',
                    'remote code execution', 'rce', 'privilege escalation', 'lateral movement',
                    'command and control', 'c2', 'c&c', 'exfiltration', 'persistence'
                ],
                'weight': 0.85
            },
            
            # Security context words
            'security_context': {
                'keywords': [
                    'compromise', 'breach', 'incident', 'attack', 'targeted', 'victims',
                    'infected', 'compromised', 'unauthorized', 'malicious', 'threat',
                    'security', 'cyber', 'hack', 'intrusion', 'suspicious'
                ],
                'weight': 0.7
            },
            
            # Technical indicators
            'technical_indicators': {
                'keywords': [
                    'ioc', 'indicator', 'hash', 'domain', 'ip address', 'registry',
                    'mutex', 'yara', 'snort', 'sigma', 'cve', 'cvss', 'patch',
                    'vulnerability scanner', 'payload', 'shellcode', 'dropper'
                ],
                'weight': 0.8
            },
            
            # Security tools and vendors
            'security_tools': {
                'keywords': [
                    'firewall', 'ids', 'ips', 'siem', 'edr', 'antivirus', 'av',
                    'sandbox', 'honeypot', 'forensics', 'incident response', 'ir',
                    'crowdstrike', 'fireeye', 'mandiant', 'kaspersky', 'symantec'
                ],
                'weight': 0.6
            },
            
            # Defensive actions
            'defensive_actions': {
                'keywords': [
                    'mitigation', 'remediation', 'detection', 'prevention', 'patch',
                    'update', 'fix', 'defend', 'protect', 'secure', 'harden',
                    'monitor', 'alert', 'investigate', 'respond', 'contain'
                ],
                'weight': 0.5
            }
        }
        
        # Context patterns that suggest security content
        self.context_patterns = [
            # Attribution patterns
            r'attributed to\s+\w+',
            r'linked to\s+\w+',
            r'associated with\s+\w+',
            r'believed to be\s+\w+',
            
            # Victim patterns
            r'targeted\s+(?:organizations?|companies|agencies)',
            r'victims?\s+(?:include|were|are)',
            r'affected\s+(?:organizations?|systems?|users?)',
            
            # Technical patterns
            r'exploits?\s+(?:a|the)?\s*(?:vulnerability|bug|flaw)',
            r'delivers?\s+(?:a|the)?\s*(?:payload|malware|trojan)',
            r'downloads?\s+(?:additional|second-stage|malicious)',
            r'executes?\s+(?:code|payload|malware)',
            
            # Investigation patterns
            r'researchers?\s+(?:discovered|found|identified)',
            r'analysis\s+(?:revealed|showed|found)',
            r'investigation\s+(?:revealed|uncovered|found)',
            
            # IoC patterns
            r'(?:ip\s+address|domain|url|hash):\s*\S+',
            r'[0-9a-fA-F]{32,64}',  # MD5/SHA hashes
            r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',  # IP addresses
            r'[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}',  # Domains
        ]
        
        # Non-security content indicators (negative signals)
        self.non_security_patterns = {
            'business': ['merger', 'acquisition', 'revenue', 'earnings', 'stock', 'market cap'],
            'product': ['release', 'launch', 'feature', 'update', 'version', 'roadmap'],
            'api_docs': ['endpoint', 'request', 'response', 'parameter', 'authentication', 'rate limit'],
            'tutorial': ['how to', 'guide', 'tutorial', 'example', 'documentation', 'getting started'],
            'marketing': ['announce', 'proud', 'excited', 'available now', 'coming soon', 'beta']
        }
        
        # Contextual security phrases (multi-word patterns)
        self.security_phrases = [
            'threat intelligence', 'security advisory', 'vulnerability disclosure',
            'incident response', 'compromise assessment', 'threat hunting',
            'attack chain', 'kill chain', 'defense evasion', 'initial access',
            'execution', 'persistence', 'privilege escalation', 'credential access',
            'discovery', 'lateral movement', 'command and control', 'exfiltration',
            'impact', 'supply chain attack', 'watering hole', 'drive-by download'
        ]
    
    def analyze_content(self, 
                       title: str, 
                       content: str, 
                       url: Optional[str] = None,
                       metadata: Optional[Dict] = None) -> ContentAnalysisResult:
        """
        Perform comprehensive content analysis to determine type and processing needs.
        
        Args:
            title: Article title
            content: Full article content
            url: Source URL (optional)
            metadata: Additional metadata (optional)
            
        Returns:
            ContentAnalysisResult with detailed analysis
        """
        
        # Combine title and content for full context
        full_text = f"{title}\n\n{content}".lower()
        
        # Perform multi-level analysis
        security_score, security_details = self._analyze_security_indicators(full_text)
        context_matches = self._find_context_patterns(full_text)
        entities = self._extract_entities(full_text)
        phrase_matches = self._check_security_phrases(full_text)
        non_security_score = self._calculate_non_security_score(full_text)
        
        # Calculate composite security confidence
        security_confidence = self._calculate_security_confidence(
            security_score, 
            len(context_matches), 
            entities,
            phrase_matches,
            non_security_score
        )
        
        # Determine content type and recommendation
        content_type, recommendation = self._determine_content_type(
            security_confidence,
            security_details,
            entities,
            non_security_score
        )
        
        # Determine analysis depth needed
        analysis_depth = self._determine_analysis_depth(
            security_confidence,
            entities,
            context_matches
        )
        
        return ContentAnalysisResult(
            content_type=content_type,
            confidence=security_confidence,
            security_indicators=security_details,
            context_clues=context_matches[:10],  # Top 10 context clues
            detected_entities=entities,
            recommendation=recommendation,
            analysis_depth=analysis_depth
        )
    
    def _analyze_security_indicators(self, text: str) -> Tuple[float, Dict[str, float]]:
        """Analyze text for security indicators with weighted scoring"""
        
        total_score = 0.0
        category_scores = {}
        word_count = len(text.split())
        
        for category, pattern_info in self.security_patterns.items():
            keywords = pattern_info['keywords']
            weight = pattern_info['weight']
            
            # Count matches
            matches = 0
            for keyword in keywords:
                # Use word boundaries for more accurate matching
                pattern = r'\b' + re.escape(keyword) + r'\b'
                matches += len(re.findall(pattern, text, re.IGNORECASE))
            
            # Normalize by text length and apply weight
            normalized_score = min(1.0, (matches / max(1, word_count / 100)) * weight)
            category_scores[category] = normalized_score
            total_score += normalized_score
        
        # Average across categories
        avg_score = total_score / len(self.security_patterns) if self.security_patterns else 0
        
        return avg_score, category_scores
    
    def _find_context_patterns(self, text: str) -> List[str]:
        """Find contextual patterns that suggest security content"""
        
        context_clues = []
        
        for pattern in self.context_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean and truncate long matches
                clean_match = match.strip()[:100]
                if clean_match and len(clean_match) > 5:  # Ignore very short matches
                    context_clues.append(clean_match)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_clues = []
        for clue in context_clues:
            if clue not in seen:
                seen.add(clue)
                unique_clues.append(clue)
        
        return unique_clues
    
    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extract potential security-related entities"""
        
        entities = {
            'threat_actors': [],
            'malware': [],
            'vulnerabilities': [],
            'ips': [],
            'domains': [],
            'hashes': [],
            'tools': []
        }
        
        # Extract IP addresses
        ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
        entities['ips'] = list(set(re.findall(ip_pattern, text)))
        
        # Extract domains (simplified)
        domain_pattern = r'\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b'
        potential_domains = re.findall(domain_pattern, text)
        # Filter out common non-malicious domains
        entities['domains'] = [d for d in set(potential_domains) 
                              if not any(d.endswith(tld) for tld in ['.com', '.org', '.net']) 
                              or any(susp in d for susp in ['malware', 'evil', 'bad', 'c2'])]
        
        # Extract hashes
        hash_patterns = {
            'md5': r'\b[a-fA-F0-9]{32}\b',
            'sha1': r'\b[a-fA-F0-9]{40}\b',
            'sha256': r'\b[a-fA-F0-9]{64}\b'
        }
        for hash_type, pattern in hash_patterns.items():
            matches = re.findall(pattern, text)
            entities['hashes'].extend(matches)
        
        # Extract CVEs
        cve_pattern = r'CVE-\d{4}-\d{4,7}'
        entities['vulnerabilities'] = list(set(re.findall(cve_pattern, text, re.IGNORECASE)))
        
        # Extract potential threat actor names (uppercase words/phrases)
        # This is a heuristic - in production you'd want a proper NER model
        actor_pattern = r'\b(?:APT\d+|FIN\d+|[A-Z][a-z]+\s*(?:Bear|Panda|Tiger|Spider|Kitten))\b'
        entities['threat_actors'] = list(set(re.findall(actor_pattern, text)))
        
        return entities
    
    def _check_security_phrases(self, text: str) -> int:
        """Check for multi-word security phrases"""
        
        phrase_count = 0
        for phrase in self.security_phrases:
            if phrase.lower() in text:
                phrase_count += 1
        
        return phrase_count
    
    def _calculate_non_security_score(self, text: str) -> float:
        """Calculate score for non-security content"""
        
        total_matches = 0
        word_count = len(text.split())
        
        for category, keywords in self.non_security_patterns.items():
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                total_matches += len(re.findall(pattern, text, re.IGNORECASE))
        
        # Normalize by text length
        return min(1.0, total_matches / max(1, word_count / 50))
    
    def _calculate_security_confidence(self,
                                     security_score: float,
                                     context_matches: int,
                                     entities: Dict[str, List[str]],
                                     phrase_matches: int,
                                     non_security_score: float) -> float:
        """Calculate overall confidence that content is security-related"""
        
        # Base score from keyword analysis
        confidence = security_score * 0.3
        
        # Boost for context matches
        context_boost = min(0.25, context_matches * 0.05)
        confidence += context_boost
        
        # Boost for security phrases
        phrase_boost = min(0.15, phrase_matches * 0.03)
        confidence += phrase_boost
        
        # Boost for entity detection
        entity_boost = 0.0
        if entities['vulnerabilities']:
            entity_boost += 0.15
        if entities['threat_actors']:
            entity_boost += 0.15
        if entities['hashes'] or entities['ips']:
            entity_boost += 0.10
        confidence += min(0.3, entity_boost)
        
        # Penalty for non-security content
        confidence -= non_security_score * 0.5
        
        # Ensure confidence is between 0 and 1
        return max(0.0, min(1.0, confidence))
    
    def _determine_content_type(self,
                              confidence: float,
                              security_details: Dict[str, float],
                              entities: Dict[str, List[str]],
                              non_security_score: float) -> Tuple[str, str]:
        """Determine content type and processing recommendation"""
        
        # High confidence security content
        if confidence >= 0.7:
            return 'security_threat', 'Use advanced threat analysis with full optimization'
        
        # Medium confidence - likely security but needs verification
        elif confidence >= 0.4:
            # Check for specific indicators
            if entities['vulnerabilities'] or entities['threat_actors']:
                return 'security_advisory', 'Use advanced analysis with focus on entity extraction'
            elif security_details.get('security_tools', 0) > 0.5:
                return 'security_tools', 'Use standard enrichment with security context'
            else:
                return 'potential_security', 'Use advanced analysis with lower confidence threshold'
        
        # Low confidence but some security indicators
        elif confidence >= 0.2:
            if non_security_score > 0.5:
                return 'mixed_content', 'Use basic enrichment with security keyword extraction'
            else:
                return 'unclear', 'Use standard enrichment with security monitoring'
        
        # Non-security content
        else:
            if non_security_score > 0.6:
                if 'api' in str(security_details):
                    return 'api_documentation', 'Use technical enrichment only'
                else:
                    return 'business_content', 'Use minimal enrichment'
            else:
                return 'general_content', 'Use basic enrichment'
    
    def _determine_analysis_depth(self,
                                confidence: float,
                                entities: Dict[str, List[str]],
                                context_matches: List[str]) -> str:
        """Determine how deep the analysis should go"""
        
        # High confidence with many indicators
        if confidence >= 0.7 and (len(context_matches) >= 5 or 
                                  any(len(v) > 0 for v in entities.values())):
            return 'comprehensive'
        
        # Medium confidence or fewer indicators
        elif confidence >= 0.4 or len(context_matches) >= 3:
            return 'detailed'
        
        # Low confidence or minimal indicators
        else:
            return 'basic'
    
    def explain_analysis(self, result: ContentAnalysisResult) -> str:
        """Generate human-readable explanation of the analysis"""
        
        explanation = []
        
        explanation.append(f"Content Type: {result.content_type}")
        explanation.append(f"Confidence: {result.confidence:.2%}")
        explanation.append(f"Analysis Depth: {result.analysis_depth}")
        
        # Top security indicators
        if result.security_indicators:
            top_indicators = sorted(result.security_indicators.items(), 
                                  key=lambda x: x[1], reverse=True)[:3]
            explanation.append("\nTop Security Indicators:")
            for indicator, score in top_indicators:
                if score > 0:
                    explanation.append(f"  - {indicator}: {score:.2%}")
        
        # Detected entities
        if any(result.detected_entities.values()):
            explanation.append("\nDetected Entities:")
            for entity_type, values in result.detected_entities.items():
                if values:
                    explanation.append(f"  - {entity_type}: {len(values)} found")
        
        # Context clues
        if result.context_clues:
            explanation.append(f"\nContext Clues Found: {len(result.context_clues)}")
            explanation.append("Examples:")
            for clue in result.context_clues[:3]:
                explanation.append(f"  - '{clue}'")
        
        explanation.append(f"\nRecommendation: {result.recommendation}")
        
        return "\n".join(explanation)


# Example usage
if __name__ == "__main__":
    analyzer = IntelligentContentAnalyzer()
    
    # Example 1: Subtle security content (APT mentioned in body, not title)
    example1 = {
        'title': 'Software Update Advisory',
        'content': '''
        A recent investigation by security researchers has uncovered a sophisticated 
        campaign targeting financial institutions. The activity has been attributed to 
        APT29, also known as Cozy Bear, based on similar TTPs observed in previous 
        operations. The threat actors are using a new variant of their custom toolset 
        to establish persistence and move laterally through victim networks.
        
        Organizations are advised to check for the following indicators:
        - Unusual PowerShell activity
        - Connections to 185.243.112.45
        - Presence of files with hash a1b2c3d4e5f6789012345678901234567890abcdef
        '''
    }
    
    result1 = analyzer.analyze_content(example1['title'], example1['content'])
    print("Example 1: Subtle Security Content")
    print(analyzer.explain_analysis(result1))
    print("-" * 80)
    
    # Example 2: API documentation (should not trigger security analysis)
    example2 = {
        'title': 'REST API v2.0 Release Notes',
        'content': '''
        We're excited to announce the release of our REST API v2.0! This update 
        includes improved authentication using OAuth 2.0, new endpoints for user 
        management, and enhanced rate limiting for better protection against abuse.
        
        Key changes:
        - New /api/v2/users endpoint
        - Deprecated /api/v1/authenticate in favor of OAuth flow
        - Added webhook support for real-time notifications
        '''
    }
    
    result2 = analyzer.analyze_content(example2['title'], example2['content'])
    print("\nExample 2: API Documentation")
    print(analyzer.explain_analysis(result2))
    print("-" * 80)
    
    # Example 3: Mixed content (business news about security company)
    example3 = {
        'title': 'Quarterly Earnings Report',
        'content': '''
        CrowdStrike reported strong Q3 earnings with revenue growth of 35% YoY.
        The cybersecurity company cited increased demand for endpoint protection
        following several high-profile ransomware attacks this quarter. CEO states
        that their Falcon platform detected and prevented over 1 million threats.
        '''
    }
    
    result3 = analyzer.analyze_content(example3['title'], example3['content'])
    print("\nExample 3: Mixed Content")
    print(analyzer.explain_analysis(result3))