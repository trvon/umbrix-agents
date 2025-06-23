"""
Training Data Generator for DSPy Optimization

This module generates high-quality training examples for optimizing DSPy modules
using BootstrapFewShot, LabeledFewShot, and KNNFewShot techniques.
"""

import json
import random
import re
import hashlib
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class ThreatIntelExample:
    """Structured threat intelligence training example"""
    content: str
    threat_classification: str
    severity: str
    iocs: List[Dict[str, Any]]
    attribution: str
    confidence_scores: Dict[str, float]
    reasoning_chain: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DSPy optimization"""
        return asdict(self)


class TrainingDataGenerator:
    """Generate and curate training data for DSPy optimization"""
    
    def __init__(self):
        self.apt_groups = [
            "APT1", "APT28", "APT29", "Lazarus", "Carbanak", "FIN7", "Turla", 
            "Comment Crew", "Equation Group", "Cozy Bear", "Fancy Bear",
            "APT40", "APT41", "Winnti", "TA505", "Sandworm", "APT33", "APT34"
        ]
        
        self.malware_families = [
            "Cobalt Strike", "Mimikatz", "PowerShell Empire", "Metasploit",
            "RAT", "Ransomware", "Banking Trojan", "Rootkit", "Backdoor",
            "Emotet", "TrickBot", "Ryuk", "Conti", "BlackMatter", "DarkSide"
        ]
        
        self.threat_types = [
            "APT Campaign", "Malware Distribution", "Phishing Campaign", 
            "Ransomware Attack", "Data Breach", "Supply Chain Attack",
            "Zero-Day Exploit", "Credential Harvesting", "Botnet Activity"
        ]
        
        self.ioc_patterns = {
            'ip': r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
            'domain': r'\b[a-zA-Z0-9-]+\.[a-zA-Z]{2,}\b',
            'hash_md5': r'\b[a-fA-F0-9]{32}\b',
            'hash_sha256': r'\b[a-fA-F0-9]{64}\b',
            'cve': r'CVE-\d{4}-\d{4,7}',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        }
    
    def create_training_examples(self, num_examples: int = 200) -> List[Dict[str, Any]]:
        """Create diverse training examples for optimization"""
        
        examples = []
        
        # High-quality manual examples (30%)
        manual_count = int(num_examples * 0.3)
        examples.extend(self._create_manual_examples(manual_count))
        
        # Synthetic examples with known patterns (50%) 
        synthetic_count = int(num_examples * 0.5)
        examples.extend(self._create_synthetic_examples(synthetic_count))
        
        # Real-world inspired examples (20%)
        realworld_count = num_examples - len(examples)
        examples.extend(self._create_realworld_examples(realworld_count))
        
        # Shuffle for good distribution
        random.shuffle(examples)
        
        return examples
    
    def _create_manual_examples(self, count: int) -> List[Dict[str, Any]]:
        """Create high-quality manual examples with detailed annotations"""
        
        manual_examples = [
            ThreatIntelExample(
                content="""
                The APT29 group, also known as Cozy Bear, has been observed using a new variant 
                of their custom malware to target government organizations. The malware, dubbed 
                "CozyDuke-v3", communicates with C2 servers at cozy-bear-c2.com and uses the 
                hash 7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730 for 
                persistence. This campaign appears to be related to CVE-2023-1234 exploitation.
                The attacks have been traced to infrastructure previously associated with SVR.
                """,
                threat_classification="APT Campaign",
                severity="high",
                iocs=[
                    {'type': 'domain', 'value': 'cozy-bear-c2.com', 'confidence': 0.95},
                    {'type': 'hash_sha256', 'value': '7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730', 'confidence': 0.98},
                    {'type': 'cve', 'value': 'CVE-2023-1234', 'confidence': 0.90}
                ],
                attribution="APT29",
                confidence_scores={
                    'classification': 0.92,
                    'attribution': 0.88,
                    'ioc_extraction': 0.94,
                    'severity': 0.85
                },
                reasoning_chain=[
                    "Identified APT29 group mention with high confidence based on naming and TTPs",
                    "Extracted C2 domain with clear malicious intent context",
                    "Found SHA256 hash in persistence mechanism context",
                    "Linked CVE exploitation to campaign infrastructure",
                    "SVR attribution aligns with known APT29 attribution patterns"
                ]
            ).to_dict(),
            
            ThreatIntelExample(
                content="""
                Multiple organizations have reported infections with a new Emotet variant that 
                drops TrickBot as a secondary payload. The initial infection vector appears to be 
                malicious Excel documents with macro functionality. IOCs include the domain 
                emotet-staging.biz (185.243.112.45) and the file hash 
                a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456. The campaign 
                targets financial institutions in the US and Europe.
                """,
                threat_classification="Malware Distribution",
                severity="high",
                iocs=[
                    {'type': 'domain', 'value': 'emotet-staging.biz', 'confidence': 0.92},
                    {'type': 'ip', 'value': '185.243.112.45', 'confidence': 0.90},
                    {'type': 'hash_sha256', 'value': 'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456', 'confidence': 0.88}
                ],
                attribution="TA542",
                confidence_scores={
                    'classification': 0.95,
                    'attribution': 0.82,
                    'ioc_extraction': 0.91,
                    'severity': 0.89
                },
                reasoning_chain=[
                    "Emotet variant identified through behavioral analysis",
                    "TrickBot secondary payload confirms multi-stage attack",
                    "Excel macro delivery method matches known Emotet TTPs",
                    "Domain and IP extracted from network indicators",
                    "Geographic targeting suggests organized campaign"
                ]
            ).to_dict(),
            
            ThreatIntelExample(
                content="""
                A sophisticated phishing campaign targeting healthcare organizations has been 
                discovered. The emails appear to come from legitimate medical suppliers but 
                contain links to credential harvesting sites at fake-medSupply.net and 
                healthcare-login.org. The campaign uses legitimate SSL certificates and 
                sophisticated social engineering to bypass email security.
                """,
                threat_classification="Phishing Campaign",
                severity="medium",
                iocs=[
                    {'type': 'domain', 'value': 'fake-medSupply.net', 'confidence': 0.94},
                    {'type': 'domain', 'value': 'healthcare-login.org', 'confidence': 0.93}
                ],
                attribution="Unknown",
                confidence_scores={
                    'classification': 0.93,
                    'attribution': 0.30,
                    'ioc_extraction': 0.96,
                    'severity': 0.78
                },
                reasoning_chain=[
                    "Phishing campaign identified through email analysis",
                    "Healthcare sector targeting suggests specialized knowledge",
                    "Legitimate SSL certificates indicate sophisticated preparation",
                    "Multiple domains suggest coordinated infrastructure",
                    "Attribution uncertain due to lack of unique TTPs"
                ]
            ).to_dict()
        ]
        
        return manual_examples[:count]
    
    def _create_synthetic_examples(self, count: int) -> List[Dict[str, Any]]:
        """Create synthetic examples with controlled variation"""
        
        synthetic_examples = []
        
        for i in range(count):
            # Randomly select threat components
            apt_group = random.choice(self.apt_groups)
            malware = random.choice(self.malware_families)
            threat_type = random.choice(self.threat_types)
            severity = random.choice(['low', 'medium', 'high', 'critical'])
            
            # Generate synthetic content
            content = self._generate_synthetic_content(apt_group, malware, threat_type, severity)
            
            # Extract IOCs from synthetic content
            iocs = self._extract_synthetic_iocs(content)
            
            # Generate confidence scores with realistic variation
            confidence_scores = self._generate_confidence_scores()
            
            # Create reasoning chain
            reasoning_chain = self._generate_reasoning_chain(apt_group, malware, threat_type)
            
            example = ThreatIntelExample(
                content=content,
                threat_classification=threat_type,
                severity=severity,
                iocs=iocs,
                attribution=apt_group,
                confidence_scores=confidence_scores,
                reasoning_chain=reasoning_chain
            ).to_dict()
            
            synthetic_examples.append(example)
        
        return synthetic_examples
    
    def _create_realworld_examples(self, count: int) -> List[Dict[str, Any]]:
        """Create examples based on real-world attack patterns"""
        
        realworld_templates = [
            {
                "template": "Recent analysis of {malware} samples reveals new TTPs including {technique}. The campaign targets {sector} organizations using {vector}. Attribution points to {actor} based on {evidence}.",
                "variables": {
                    "malware": ["Cobalt Strike", "Emotet", "TrickBot", "Ryuk"],
                    "technique": ["DLL sideloading", "process hollowing", "credential dumping"],
                    "sector": ["financial", "healthcare", "government", "energy"],
                    "vector": ["spear phishing", "watering hole attacks", "supply chain compromise"],
                    "actor": ["APT28", "APT29", "FIN7", "Lazarus"],
                    "evidence": ["code overlap", "infrastructure reuse", "timing patterns"]
                }
            }
        ]
        
        examples = []
        
        for i in range(count):
            template = random.choice(realworld_templates)
            content = self._fill_template(template)
            
            # Extract information for labels
            threat_type = random.choice(self.threat_types)
            severity = random.choice(['medium', 'high', 'critical'])
            attribution = random.choice(self.apt_groups)
            
            example = ThreatIntelExample(
                content=content,
                threat_classification=threat_type,
                severity=severity,
                iocs=self._extract_synthetic_iocs(content),
                attribution=attribution,
                confidence_scores=self._generate_confidence_scores(),
                reasoning_chain=self._generate_reasoning_chain(attribution, "mixed", threat_type)
            ).to_dict()
            
            examples.append(example)
        
        return examples
    
    def _generate_synthetic_content(self, apt_group: str, malware: str, threat_type: str, severity: str) -> str:
        """Generate synthetic threat intelligence content"""
        
        templates = [
            f"Security researchers have identified a new {threat_type.lower()} attributed to {apt_group}. The attack uses {malware} to compromise targets through sophisticated social engineering. The campaign demonstrates {severity} severity due to its widespread impact.",
            f"Analysis of recent {malware} samples reveals connections to {apt_group} operations. This {threat_type.lower()} targets critical infrastructure with {severity} impact potential.",
            f"{apt_group} has been observed deploying {malware} in a coordinated {threat_type.lower()}. The attack methodology suggests {severity} threat level."
        ]
        
        base_content = random.choice(templates)
        
        # Add synthetic IOCs
        synthetic_iocs = []
        if random.random() > 0.3:  # 70% chance of domain
            synthetic_iocs.append(f"evil-{random.randint(100, 999)}.com")
        if random.random() > 0.5:  # 50% chance of IP
            synthetic_iocs.append(f"{random.randint(185, 195)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}")
        if random.random() > 0.4:  # 60% chance of hash
            synthetic_iocs.append(hashlib.sha256(f"synthetic_{random.randint(1000,9999)}_{apt_group}".encode()).hexdigest())
        
        # Embed IOCs in content
        if synthetic_iocs:
            base_content += f" IOCs include: {', '.join(synthetic_iocs[:2])}."
        
        return base_content
    
    def _extract_synthetic_iocs(self, content: str) -> List[Dict[str, Any]]:
        """Extract IOCs from synthetic content with confidence scores"""
        
        iocs = []
        
        for ioc_type, pattern in self.ioc_patterns.items():
            matches = re.findall(pattern, content)
            for match in matches:
                confidence = random.uniform(0.7, 0.98)  # Realistic confidence range
                iocs.append({
                    'type': ioc_type,
                    'value': match,
                    'confidence': round(confidence, 2)
                })
        
        return iocs
    
    def _generate_confidence_scores(self) -> Dict[str, float]:
        """Generate realistic confidence scores with variation"""
        
        base_confidence = random.uniform(0.6, 0.95)
        variation = 0.15
        
        return {
            'classification': round(max(0.1, min(1.0, base_confidence + random.uniform(-variation, variation))), 2),
            'attribution': round(max(0.1, min(1.0, base_confidence + random.uniform(-variation, variation))), 2),
            'ioc_extraction': round(max(0.1, min(1.0, base_confidence + random.uniform(-variation, variation))), 2),
            'severity': round(max(0.1, min(1.0, base_confidence + random.uniform(-variation, variation))), 2)
        }
    
    def _generate_reasoning_chain(self, apt_group: str, malware: str, threat_type: str) -> List[str]:
        """Generate realistic reasoning chains"""
        
        reasoning_templates = [
            f"Identified {threat_type.lower()} based on attack pattern analysis",
            f"Attribution to {apt_group} based on TTP similarity",
            f"Malware classification as {malware} through behavioral analysis",
            f"IOC extraction using pattern matching and context analysis",
            f"Confidence assessment based on evidence quality and source reliability"
        ]
        
        # Add some variation
        chain_length = random.randint(3, 6)
        return random.sample(reasoning_templates, min(chain_length, len(reasoning_templates)))
    
    def _fill_template(self, template_dict: Dict) -> str:
        """Fill template with random variables"""
        
        template = template_dict["template"]
        variables = template_dict["variables"]
        
        for var_name, var_options in variables.items():
            value = random.choice(var_options)
            template = template.replace(f"{{{var_name}}}", value)
        
        return template
    
    def create_validation_set(self, num_examples: int = 100) -> List[Dict[str, Any]]:
        """Create high-quality validation set for testing"""
        
        validation_examples = []
        
        # Complex multi-step attacks (30%)
        validation_examples.extend(self._create_complex_attack_examples(int(num_examples * 0.3)))
        
        # Attribution challenges (25%)
        validation_examples.extend(self._create_attribution_challenge_examples(int(num_examples * 0.25)))
        
        # False positive scenarios (20%)
        validation_examples.extend(self._create_false_positive_examples(int(num_examples * 0.2)))
        
        # Novel/unknown threats (25%)
        remaining = num_examples - len(validation_examples)
        validation_examples.extend(self._create_novel_threat_examples(remaining))
        
        return validation_examples
    
    def _create_complex_attack_examples(self, count: int) -> List[Dict[str, Any]]:
        """Create complex multi-stage attack examples"""
        
        examples = []
        for i in range(count):
            content = f"""
            A sophisticated multi-stage attack has been identified targeting financial institutions.
            Stage 1: Spear phishing emails with malicious attachments delivered to executives
            Stage 2: Initial payload drops {random.choice(self.malware_families)} for persistence
            Stage 3: Lateral movement using stolen credentials and {random.choice(['PsExec', 'WMI', 'RDP'])}
            Stage 4: Data exfiltration to {self._generate_fake_domain()}
            Attribution analysis suggests {random.choice(self.apt_groups)} involvement.
            """
            
            examples.append(ThreatIntelExample(
                content=content,
                threat_classification="APT Campaign",
                severity="critical",
                iocs=self._extract_synthetic_iocs(content),
                attribution=random.choice(self.apt_groups),
                confidence_scores=self._generate_confidence_scores(),
                reasoning_chain=[
                    "Multi-stage attack pattern identified",
                    "Financial sector targeting suggests specific motivation",
                    "Sophisticated TTPs indicate advanced threat actor",
                    "Attribution based on infrastructure and technique overlap"
                ]
            ).to_dict())
        
        return examples
    
    def _create_attribution_challenge_examples(self, count: int) -> List[Dict[str, Any]]:
        """Create examples with challenging attribution scenarios"""
        
        examples = []
        for i in range(count):
            content = f"""
            Security incident analysis reveals attack techniques consistent with multiple threat actors.
            Observed TTPs include {random.choice(['DLL sideloading', 'process hollowing', 'credential dumping'])}
            which has been used by both {random.choice(self.apt_groups)} and {random.choice(self.apt_groups)}.
            Infrastructure analysis shows shared hosting patterns but with operational security measures
            that obscure definitive attribution.
            """
            
            examples.append(ThreatIntelExample(
                content=content,
                threat_classification="Attribution Challenge",
                severity="medium",
                iocs=self._extract_synthetic_iocs(content),
                attribution="Unknown",
                confidence_scores={
                    'classification': 0.85,
                    'attribution': 0.25,  # Low attribution confidence
                    'ioc_extraction': 0.80,
                    'severity': 0.75
                },
                reasoning_chain=[
                    "Multiple potential threat actors identified",
                    "TTP overlap creates attribution uncertainty",
                    "Infrastructure analysis inconclusive",
                    "Requires additional indicators for attribution"
                ]
            ).to_dict())
        
        return examples
    
    def _create_false_positive_examples(self, count: int) -> List[Dict[str, Any]]:
        """Create examples that might be false positives"""
        
        examples = []
        for i in range(count):
            content = f"""
            Network monitoring detected unusual traffic patterns to external domain {self._generate_fake_domain()}.
            Investigation reveals legitimate business application connecting to cloud service provider.
            Initial alert triggered due to geographic location and encryption protocols used.
            Further analysis confirms benign nature of the traffic.
            """
            
            examples.append(ThreatIntelExample(
                content=content,
                threat_classification="False Positive",
                severity="low",
                iocs=[],
                attribution="N/A",
                confidence_scores={
                    'classification': 0.90,
                    'attribution': 0.0,
                    'ioc_extraction': 0.20,
                    'severity': 0.95
                },
                reasoning_chain=[
                    "Initial suspicious activity detected",
                    "Investigation revealed legitimate business use",
                    "Geographic and protocol factors caused false alert",
                    "Confirmed benign through detailed analysis"
                ]
            ).to_dict())
        
        return examples
    
    def _create_novel_threat_examples(self, count: int) -> List[Dict[str, Any]]:
        """Create examples of novel or unknown threats"""
        
        examples = []
        for i in range(count):
            content = f"""
            Novel attack technique observed using previously unknown vulnerability.
            Exploit targets {random.choice(['IoT devices', 'cloud infrastructure', 'mobile applications'])}
            with sophisticated evasion techniques. Initial discovery through anomaly detection.
            Attribution unknown but demonstrates advanced capabilities.
            """
            
            examples.append(ThreatIntelExample(
                content=content,
                threat_classification="Novel Threat",
                severity=random.choice(['medium', 'high']),
                iocs=self._extract_synthetic_iocs(content),
                attribution="Unknown",
                confidence_scores={
                    'classification': 0.75,  # Lower confidence for novel threats
                    'attribution': 0.15,
                    'ioc_extraction': 0.70,
                    'severity': 0.80
                },
                reasoning_chain=[
                    "Novel attack pattern identified",
                    "Previously unknown vulnerability exploited",
                    "Advanced evasion techniques observed",
                    "Attribution requires further investigation"
                ]
            ).to_dict())
        
        return examples
    
    def _generate_fake_domain(self) -> str:
        """Generate realistic fake domain names"""
        prefixes = ['secure', 'cloud', 'api', 'cdn', 'mail', 'app', 'data']
        suffixes = ['service', 'platform', 'systems', 'tech', 'solutions']
        tlds = ['com', 'net', 'org', 'io', 'co']
        
        return f"{random.choice(prefixes)}-{random.choice(suffixes)}.{random.choice(tlds)}"
    
    def save_training_data(self, examples: List[Dict], filename: str) -> None:
        """Save training data to JSON file"""
        
        output_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_examples': len(examples),
                'generator_version': '1.0'
            },
            'examples': examples
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"Saved {len(examples)} training examples to {filename}")


if __name__ == "__main__":
    # Generate training and validation data
    generator = TrainingDataGenerator()
    
    # Create training set
    training_examples = generator.create_training_examples(200)
    generator.save_training_data(training_examples, "training_data.json")
    
    # Create validation set
    validation_examples = generator.create_validation_set(100)
    generator.save_training_data(validation_examples, "validation_data.json")
    
    print("Training data generation completed successfully!")