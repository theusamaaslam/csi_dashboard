# csi_utils.py
import os
import gc
import pandas as pd
import numpy as np
import re
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def worker(batch_ids, data_dict, config):
    scorer = CSIScorer()
    return process_customer_batch(batch_ids, data_dict, config, scorer)

class CPTOptimizedCSIConfig:
    """
    Configuration for CPU-optimized CSI model, including file paths,
    processing parameters, and time-based recency windows.
    """
    
    def __init__(self):
        # CPU-optimized processing parameters
        self.processing_batch_size = 5000 
        
        # Memory management
        self.enable_gc = True
        
        # Column mappings (standardized to lowercase with underscores)
        self.call_columns = {
            'userid': 'userid',
            'entry_time': 'entry_time',
            'duration': 'call_duration',
            'category': 'call_detail_log_group',
            'call_fault_type': 'master_fault_type',
            'call_sub_type': 'sub_fault_type'
        }
        
        self.ticket_columns = {
            'userid': 'userid',
            'ticket_type': 'ticket_type',
            'fault_type': 'fault_types',
            'sub_fault_type': 'sub_fault_types',
            'duration': 'duration',
            'creation_time': 'creation_time'
        }
        
        self.outage_columns = {
            'userid': 'userid',
            'duration': 'duration',
            'event_type': 'event_type',
            'start_time': 'occurrence_time'
        }
        
        self.activity_columns = {
            'userid': 'userid',
            'downtime_hours': 'customer_downtime_hours',
            'start_time': 'occurrence_time',
            'activity_type': 'services',
            'status': 'status'
        }
        
        # Recency windows for time-based frequency penalties (in days)
        self.recency_windows_days = [7, 30, 90, 180]

class CSIScorer:
    """
    Defines impact weights for various customer interaction categories
    to calculate penalties for the CSI score.
    """
    
    def __init__(self):
        # Default penalties
        self.default_ticket_impact = -1.0
        self.default_fault_impact = -2.0
        self.default_sub_fault_impact = -1.0
        self.default_call_category_impact = -1.0
        self.default_call_fault_impact = -2.0 
        self.default_call_sub_type_impact = -1.0 
        self.default_activity_type_impact = -1.0 

        # Outage Type Multipliers
        self.outage_type_multiplier = {
            'Full Outage': 1.0,
            'Partial Service Outage': 0.6,
            'Planned Maintenance': 0.2,
            'Fiber cut': 1.0
        }
        self.default_outage_type_multiplier = 0.5

        # Ticket Type Impacts
        self.ticket_type_impact = {
            'Complaint': -5.0, 'Requirements/VAS': -2.0, 'Requirement/VAS': -2.0,
            'Requirement': -2.0, 'RequirementsAdmin': -2.0, 'CUSTOMER': -4.0,
            'Service Provisioning': -2.5, 'Service provisioning': -2.5,
            'Provisioning': -2.5, 'RT': -1.5, 'Code Baring': -1.5,
            'Voice-Locking': -2.0, 'Feedback Complaint': -3.0, 'Auto TT': -1.5,
            'Video-Provisioning': -2.0, 'Voice-Provisioning': -2.0,
            'Hardware Removal': -2.0
        }
        
        # Fault Severity Impacts
        self.fault_severity_impact = {
            'ONT': -5.0, 'Physical Link': -5.0, 'No Browse': -5.0, 'Slow Browse': -5.0,
            'POTs/SIP-POTs': -2.5, 'Web Based Ticket': -3.0, 'QC Issues': -0.5,
            'Permanent Account Closure': -3.5, 'Hardware Removal': -1.0, 'Outage': -5.0,
            'INTERNET': -4.5, 'Cabling': -1.0, 'Video-Provisioning': -0.5, 'Billing': -0.5,
            'Temporary Account Closure': -0.5, 'Basic Cable TV': -2.0,
            'Account Re-activation': -0.5, 'billing': -0.5, 'Frequent Disconnections': -4.0,
            'Hardware-Required': -1.5, 'Joy Box': -2.5, 'Router': -3.0, 'Activity': -2.0,
            'Plan Subscription': -0.5, 'Activation from Temporary Closure': -0.5, 'OTHER': -1.0,
            'Voice-Provisioning': -0.5, 'Package Change': -0.5, 'Voice Provisioning': -0.5,
            'Requirements/VAS': -2.0, 'Dark Fiber': -5.0, 'Video-Termination': -1.0,
            'Internet-Provisioning': -0.5, 'Hardware-Relocation': -0.5,
            'Security Cheque Collection': -1.0, 'BASIC-CABLE-TV': -2.0, 'sales': -1.0,
            'Systems-Provisioning': -0.5, 'Digital CAS': -2.5, 'nWatch': -2.0,
            'Installation': -1.5, 'POTS': -2.5, 'Activation from PAC(In Process)': -2.0,
            'Video Termination': -0.5, 'Non Payment': -0.5, 'Password': -1.5,
            'Activation from Non Payment': -0.5, 'Sales': -1.0, 'Nwatch': -2.0, 'NayaTV': -2.0,
            'HDBox': -2.0, 'HOSTEX': -3.5, 'NAYATV': -2.0, 'Service Unlocking': -0.5,
            'Voice-Addition/Deletion/Change': -0.5, 'JOYBOX': -2.0, 'Service Locking': -0.5,
            'Hanging': -2.0, 'VOD': -2.5, 'Service Locking/Unlocking': -1.0, 'Auto TT': -3.0,
            'CORE-Provisioning': -2.0, 'EVIEW': -2.5, 'eView': -2.5, 'Voice-Shifting': -3.0,
            'DIGITAL-BOX-HD': -2.0, 'Video-Downgrade': -0.5, 'Technical Survey': -3.0,
            'ATV Box 4K SEI': -2.5, 'CVAS': -2.0, 'Gaming Issue': -2.0, 'Configurations': -2.5,
            'Hardware Replacement': -1.0, 'SAFEWEB': -2.5, 'Status Conflict': -2.0,
            'Customer Retention': -2.0, 'Complaint': -5.0, 'NWATCH': -2.0,
            'Service locking unlocking': -2.0, 'Physical Layer': -5.0, 'OPTIMUS': -3.0,
            'UPS': -3.0, 'Testing': -1.5, 'Follow Up': -1.5, 'LIVE-STREAMING': -2.5,
            'Layer 2 Circuit': -4.0, 'Additional Usage': -1.5, 'Customer Portal': -2.0,
            'SCM': -2.0, 'Systems-Locking': -3.0, 'Termination': -3.0,
            'UNLIMITED_BUNDLE_PLUS': -1.5, 'Speed Up': -2.0, 'Optimus': -3.0,
            'Non Dark Fiber': -3.0, 'SIP-Trunk': -3.5, 'Phone App': -1.5,
            'Systems Termination': -3.5, 'Parental Locking': -1.5, 'Port Forwading': -2.0,
            'Unlimited Bundle': -1.5, 'Managed Services': -2.0, 'PREMIUM-INTERNET': -3.5,
            'Systems-Termination': -3.5, 'Development': -1.0, 'Waiver Expiry Time': -1.0,
            'Video-Locking': -2.0, 'Core Audit': -2.5, 'Video': -2.0, 'Voice-Termination': -3.5,
            'Extended Warranty': -1.5, 'NMonitor': -2.0, 'Eview': -2.5, 'WEB BASED TICKET': -3.0,
            'My Nayatel App': -1.5, 'BANDWIDTH-ON-DEMAND': -3.5, 'Smart Nwatch': -2.0,
            'JOYAPP-AndroidTV': -2.0, 'WEB-HOSTING': -2.0, 'Extended Mac': -1.0, 'EXITLAG': -1.0,
            'testing': -1.0, 'Digital Signage Box': -2.0, 'SMART_NWATCH': -2.0,
            'Hardware Testing': -2.0, 'JOYAPP': -2.0, 'Layer 3 Circuit': -4.0,
            'Core Locking': -3.5, 'Nwall': -2.0, 'Service Termiantion': -3.0, 'E-FAX': -2.0,
            'E-Fax': -2.0, 'EXTENDED-WARRANTY': -1.5, 'IP Pool': -2.0,
            'UNLIMITED_BUNDLE_EXTREME': -1.5, 'Service Provisioning': -3.0, 'Web Hosting': -2.0,
            'Email Issues': -2.0, 'SIP-POTS': -2.5, 'Internet-Termination': -3.5, 'PRI': -2.0,
            'MONTHLYRENTAL': -1.0, 'Internet-Locking': -2.5, 'DARKFIBER': -5.0,
            'Service Desk': -1.5, 'QC Visit required': -2.0, 'Video-Shifting': -2.5,
            'Video-PlanUnassigment': -2.0, 'Service Un-Locking': -2.0, 'LIT FIBER': -5.0,
            'HDBOX': -2.0, 'Under Installation': -2.0, 'KASPERSKY TOTAL SECURITY': -1.0,
            'SIP-TRUNK': -3.5, 'HDBOX-SMART': -2.0, 'Safeweb': -2.5, 'UNLIMITED_BUNDLE': -1.5,
            'Account Suspended': -2.5, 'Live TV': -2.0, 'Unlimited bundle': -1.5,
            'SIP-TRG': -3.0, 'Activation from PAC(Completed)': -2.0, 'ExitLag': -1.0,
            'NWatch': -2.0, 'Switch': -2.0, 'CORE-Shifting': -3.5, 'OSP Incident': -4.0
        }
        
        # Sub-Fault Impacts
        self.sub_fault_impact = {
            'RED': -5.0, 'NAP to Customer End Fiber Break': -5.0, 'DOWN': -5.0,
            'Limited/No Connectivity Error on LAN': -3.0, 'NAP Issue': -5.0, 'internet': -4.0,
            'FTTH Module': -3.0, 'Wireless on ONT': -3.0, 'Shifting case': -1.0, 'OFF': -0.5,
            'Down': -5.0, 'billing': -1.0, 'Low Optical Power': -3.0, 'Replacement': -1.0,
            'Wireless': -2.0, 'POTS': -2.0, 'Internet Cabling Required': -1.0, 'Internet Down': -5.0,
            'Access Point': -2.5, 'other': -1.0, 'Permanent Removal': -1.0, 'Slow Browse': -3.0,
            'No Browse': -5.0, 'Router': -1.0, 'Customer End': -0.5, 'Fiber Cut': -5.0,
            'Video Cabling Required': -1.0, 'sales': -1.0, 'Relocation': -1.0, 'Permanently': -1.0,
            'LAN': -1.0, 'Temporary Account Closure': -1.0, 'Basic Cable': -1.0,
            'LOW OPTICAL POWERS': -3.0, 'ONT': -5.0, 'Internet_Home': -1.0, 'basic-cable-tv': -2.0,
            'BASIC-CABLE-TV': -2.0, 'Cable at Low Height': -1.0, 'WAN': -3.0, 'Termination': -1.0,
            'Single/Few Website/App Issue': -2.0, 'Wireless on AP': -2.0,
            'Refund-Permanent Account Closure': -1.0, 'Configurations Issue': -2.0,
            'Wireless Signal Strength Issue': -3.0, 'AP configurations': -2.0,
            'Advance Payment': -2.0, 'JOYBOX': -2.0, 'INTERNET': -4.0, 'Snowing': -1.0,
            'Joy box on full payment': -1.0, 'NAP End': -4.0,
            'Refund-Installation Cancelled Case': -1.0, 'Single/Multiple Website Issue': -2.0,
            'Fiber Break': -5.0, 'Configurations': -2.0, 'DIGITAL-BOX-HD': -1.0,
            'Hardware Invoice': -1.0, 'Mini-ODF Issue': -4.0, 'Upload/Download Issue': -2.0,
            'Device Malfunctioning': -2.0, 'Channels Missing': -1.0, 'Not Turning On': -2.0,
            'Cable Tagging': -1.0, 'RED Blinking': -4.0, 'Single Device Issue': -1.0,
            'HDBOX': -1.0, 'DC End': -1.0, 'nayatv': -1.0, 'Telephone Cabling Required': -1.0,
            'Subscription': -1.0, 'Digital Box HD on full payment': -1.0, 'DC Issue': -4.5,
            'Fiber Rerouting within Premises': -2.5, 'CPE Compromised': -1.0, 'Mini-BPR Issue': -4.0,
            'joybox': -2.0, 'HDBOX-SMART': -2.0, 'Joy box Remote': -1.0, 'DC to NAP Fiber Break': -5.0,
            'GREEN Blinking': -1.0, 'Domain Registration': -1.0, 'Wireless on External Router': -2.0,
            'ATV Box 4K SEI on Installment': -1.0, 'Sorry No Internet Connection': -5.0,
            'Camera Down/Not Connected': -3.0, 'Web Hosting': -4.0,
            'ATV Box 4K SEI on full price': -1.0, 'eView': -3.0,
            'ATV Box 4K SEI Replacement on Full': -1.0,
            'Getting Stuck on Main Logo/Screen': -2.0, 'No Video on Single TV': -1.0,
            'Unfortunately Nayatel Joy Has Stopped Working': -2.0, 'Billing Query': -1.0,
            'Display Issue (HDMI/Front Panel)': -1.0, 'Cable Tracing': -1.0, 'STB Remote': -1.0,
            'EdgeONT WIFI6': -1.0, 'Customer Link Choking': -1.0, 'pots': -1.0,
            'Refund-Excess Amount': -1.0, 'Remote not Working Properly': -1.0,
            'Joy box on installments': -1.0, 'POP to DC Fiber Break': -5.0,
            'Address Correction': -1.0, 'digital-box-hd': -1.0,
            'Single/Multiple APPs not working': -2.0, 'Digital Box on full payment': -1.0,
            'Power Adapter': -1.0, 'Firmware Issue': -2.0, 'Outgoing Issue': -1.0,
            'Not Turning on': -2.0, 'DJS Issue': -1.0, 'optimus': -1.0,
            'Digital Box on installments': -1.0, 'BPR at Low Height': -1.0,
            'Limited/No Connectivity Error on Wireless': -3.0, 'Wireless Not Working': -2.0,
            'Temperately': -1.0, 'Channels Not Streaming': -1.0, 'Outdoor Cable Rerouting': -1.0,
            'nwatch': -1.0, 'Special Number': -1.0, 'Channels not Streaming': -1.0,
            'Joy Box on NTL-Ownership': -1.0, 'WiFi/Router Password Issue': -1.0,
            'Status Conflict': -1.0, 'Replacement Free of cost': -1.0, 'DDOS Mitigation Policy': -1.0,
            'Joint Issue': -1.0, 'IP Pool': -1.0, 'ODF Issue': -1.0, 'live-streaming': -1.0,
            'Modification': -1.0, 'Hostex': -1.0, 'Feedback': -1.0, 'NAYATEL CLOUD': -1.0,
            'No Video on Multiple TVs': -1.0, 'Wireless Signal Strength Issue on AP': -3.0,
            'Noise in line': -2.0, 'Call Feature Problem': -1.0, 'Wireless ??? Speed test Issue': -2.0,
            'Dialing Router Configurations': -2.0, 'Display Issue': -1.0, 'Activation Code Error': -1.0,
            'CPE Configurations': -2.0, 'Password': -1.0, 'Service deactivation request': -1.0,
            'DJ Issue': -1.0, 'Not Accessible': -1.0, 'SSL Certificate': -1.0,
            'Digital Box HD on Installments without Security Cheque': -1.0, 'Phone APP': -1.0,
            'unlimited_bundle_plus': -1.0, 'Incoming Issue': -1.0, 'Civil Verification': -1.0,
            'Wireless A?A?A? Speed test Issue': -2.0, 'Block International dialing only': -1.0,
            'Switch': -1.0, 'Optical fiber cable at low height': -1.0, 'CAS Issue': -1.0,
            'SIP-POTS': -1.0, 'Disable Call Blocking': -1.0, 'Permanent account closure request': -1.0,
            'Billing Issue': -1.0, 'HDBox Remote': -1.0, 'Uplink Issue': -1.0,
            'Issuance of Hardware on Invoic': -1.0, 'DIGITAL BOX HD NEW': -1.0,
            'PREMIUM-INTERNET': -1.0, 'Pre-Installation Survey': -0.5, 'exitlag': -1.0,
            'Faulty': -1.0, 'Standard Installation': -0.5, 'Congestion in destination': -1.0,
            'Display/HDMI issue': -1.0, 'JSTB100 Error': -1.0,
            'Numbers Removed in an Activity': -1.0, 'ATV Box 4K SEI Replacement on Installment': -1.0,
            'Block all outgoing': -1.0, 'Maintenance Survey': -0.5, 'Camera Addition': -1.0,
            'Shifting': -1.0, 'Entry Point to Customer End Mini ODF Fiber Break': -5.0,
            'Block Nationwide Cell phone and international dialing': -1.0, 'E_mail Hosting': -4.0,
            'NMAIL': -4.0, 'bandwidth-on-demand': -1.0, 'Horizontal  Lines': -2.0,
            'Package Testing': -0.5, 'High Latency on PC': -1.5, 'Internet': -1.0,
            'Cable Faulty': -1.0, 'Service activation request': -0.5, 'Configuration': -2.0,
            'DOMAIN': -1.0, 'Network Maintenance Required': -5.0,
            'Optical fiber cable fell on road': -5.0,
        }
        
        # Call Category Impacts
        self.call_category_impact = {
            'Complaint': -5.0, 'Revenue': -0.5, 'Level 2 (From NTL Teams)': -2.0,
            'Already Launched TT': -1.0, 'Sales': -0.5, 'Missed CTI': -2.0,
            'TechnicalDepartment': -2.0, 'Unplanned Outage': -1.0, 'Requirements/VAS': -0.5,
        }

        # Call Fault Type Impacts
        self.call_fault_type_impact = {
            'Slow Browse': -5.0, 'No Browse': -5.0, 'ONT': -5.0, 'Joy Box': -1.0,
            'My IP/My Package/My NTL No': -2.0, 'Basic Cable TV': -2.0, 'Services Down': -5.0,
            'Password': -1.0, 'Unnecessary Call': -1.0, 'HDBox': -2.0, 'Hardware Info': -0.5,
            'Account Status Locked': -1.0, 'NayaTV': -2.0, 'Account Information Update': -0.5,
            'Invoice Information': -0.5, 'Digital CAS': -2.0, 'Cabling': -2.0, 'POTs/SIP-POTs': -3.0,
            'Frequent Disconnections': -4.0, 'Transmission Activity': -4.0, 'New Service': -0.05,
            'Package Info': 0, 'Router': -2.0, 'Email Issues': -4.0, 'nWatch': -3.0,
            'Physical Link': -5.0, 'Installations': -3.0, 'Auto Bonus': 0, 'ATV Box 4K SEI': -1.0,
            'Shifting Case': -2.0, 'eView': -2.5, 'Call Dropped': 0, 'Call Transferred': 0,
            'ONT VAS Profile/AMS changes': -2.5, 'ONT Issue and Link Info': -4.5,
            'Hardware-Relocation': -2.0, 'Web Hosting': -1.0, 'New Connection': 0,
            'Package Change': 0, 'Hardware-Required': -0.5, 'Customer Portal': 0, 'Billing': -2.0,
            'Account closure': 0, 'Gaming Issue': -2.0, 'My Nayatel App': -1.0,
            'Parental Locking': -1.0, 'Follow Up': -4.0, 'Transmission activity': -2.0,
            'UPS': -0.5, 'HOSTEX': -4.0, 'VOD': -2.0, 'Speed Up': -1.0, 'Hanging': -3.0
        }

        # Call Sub-Type Impacts
        self.call_sub_type_impact = {
            'Wireless on ONT': -2.5, 'None': -0.5, 'Wireless': -2.0, 'RED': -5.0, 'OFF': -5.0,
            'Down': -5.0, 'Access Point': -3.0, 'Single Device Issue': -1.5, 'Internet Down': -5.0,
            'Wireless on AP': -2.5, 'Complaints': -3.0, 'Activation Code Error': -2.5, 'LAN': -2.0,
            'Internet': -3.0, 'Channels Missing': -3.5, 'Video': -2.0,
            'Single/Multiple Website Issue': -2.0, 'ONT': -3.5, 'Configurations Issue': -3.0,
            'Low Optical Power': -4.5, 'Single/Few Website/App Issue': -2.5, 'Snowing': -1.0,
            'Port Verification': -2.0, 'Congestion in destination': -4.0,
            'Getting Stuck on Main Logo/Screen': -3.0, 'Single/Multiple APPs not working': -3.0,
            'Not Turning On': -4.0, 'Internet Cabling Required': -3.0,
            'Display Issue (HDMI/Front Panel)': -2.0, 'Sorry No Internet Connection': -4.5,
            'POTS': -3.0, 'Urgent Installation/Working Require': -3.5, 'NWATCH': -2.5,
            'Customer Area': -1.0, '0800': -1.0, 'AP configurations': -2.5, 'Router': -3.0,
            'Camera Down/Not Connected': -3.5, 'Channels not Streaming': -3.0,
            'Wireless on External Router': -2.5, 'Video Cabling Required': -2.0,
            'Not Turning on': -4.0, 'Relocation': -2.5,
            'Limited/No Connectivity Error on Wireless': -3.5, 'Transmission': -3.5,
            'FUP Downgrade': -2.0, 'WAN': -2.5, 'Wireless Signal Strength Issue': -2.5,
            'Channels Not Streaming': -3.0, 'Customer Link Choking': -4.0, 'Port Routed/Bridged': -2.0,
            'Outage Verification': -3.5,
            'Unfortunately Nayatel Joy Has Stopped Working': -3.5,
            'Remote not Working Properly': -1.5, 'CAS Issue': -3.0, 'Display/HDMI issue': -2.5,
            'Replacement': -2.0, 'Upload/Download Issue': -3.0, 'Multiple Channels Freezing': -3.5,
            'OandM': -2.0, 'Live Not Working': -3.0, 'Outlook configurations': -2.0,
            'CPE Compromised': -4.5, 'Firmware Issue': -3.0,
            'Fiber Rerouting within Premises': -2.5, 'Channel Stucking': -3.0, 'L2-VPN/L3-VPN': -3.5,
            'No Video on Single TV': -2.5, 'Level-1': -2.0, 'Horizontal  Lines': -1.5,
            'Installation script error': -2.5, 'CPE Configurations': -3.0, 'Level-2': -3.0
        }

        # Activity Type Impacts
        self.activity_type_impact = {
            'BASIC-CABLE-TV': 0.4, 'all': 1.0, 'VIDEO': 0.3, 'POTS': 0.2, 'INTERNET': 1.0,
            'JOYBOX': 0.2, 'NAYATV': 0.2, 'EVIEW': 0.01, 'HOSTEX': 1.0, 'NAYATEL CLOUD': 0.8,
            'NAYATEL_VPN': 0.8, 'NextStorage': 0.1, 'MANAGEDSERVICES': 0.2, 'DARKFIBER': 0.8,
            'NWATCH': 0.2,
        }

def load_data_optimized(config):
    """
    Loads data directly from the AI POSTGRESQL database.
    """
    print(f"Loading data directly from AI Database...")
    data_dict = {}
    
    # We need to import the ai_engine to fetch data
    try:
        from db import ai_engine
    except ImportError:
        print("❌ CRITICAL ERROR: Could not import ai_engine from db.py")
        return data_dict

    queries = {
        'calls': 'SELECT userid, entry_time, call_duration, call_detail_log_group, master_fault_type, sub_fault_type FROM ai.cti',
        'tickets': 'SELECT userid, ticket_type, fault_types, sub_fault_types, duration, creation_time FROM ai.trouble_tickets',
        'outages': 'SELECT userid, duration, event_type, occurrence_time FROM ai.outages',
        'activities': "SELECT userid, customer_downtime_hours, occurrence_time, services, status FROM ai.activity WHERE status IN ('COMPLETED', 'PENDING', 'SUBMITTED')"
    }
    
    for data_type, query in queries.items():
        print(f"Executing query for {data_type} data...")
        try:
            # We use Pandas read_sql to load directly into a dataframe
            data_dict[data_type] = pd.read_sql(query, con=ai_engine)
            print(f"✓ {data_type.title()} Data: {len(data_dict[data_type]):,} records")
        except Exception as e:
            print(f"❌ Error loading {data_type} from DB: {e}")
            data_dict[data_type] = pd.DataFrame()
        
        if config.enable_gc:
            gc.collect()
            
    return data_dict

def preprocess_data_optimized(data_dict, config):
    """
    Preprocesses data, including standardizing columns, cleaning strings,
    and converting durations and dates.
    """
    print("Preprocessing data...")
    
    for data_type, data_df in data_dict.items():
        if data_df.empty:
            continue
            
        print(f"  Processing {data_type} data ({len(data_df):,} records)...")

        # Standardize column names to be lowercase with underscores
        data_df.columns = data_df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Clean string columns
        string_cols = data_df.select_dtypes(include=['object']).columns
        for col in string_cols:
            data_df[col] = data_df[col].astype(str).str.replace('"', '').str.strip()
        
        if data_type == 'calls':
            duration_col = config.call_columns.get('duration')
            if duration_col in data_df.columns:
                data_df['duration_hours_numeric'] = pd.to_numeric(data_df[duration_col], errors='coerce').fillna(0) / 60.0
            
            date_col = config.call_columns.get('entry_time')
            if date_col in data_df.columns:
                data_df[date_col] = pd.to_datetime(data_df[date_col], errors='coerce')
                data_df.dropna(subset=[date_col], inplace=True)

        elif data_type == 'tickets':
            duration_col = config.ticket_columns.get('duration')
            if duration_col in data_df.columns:
                data_df['duration_hours_numeric'] = pd.to_numeric(data_df[duration_col], errors='coerce').fillna(0)
            
            date_col = config.ticket_columns.get('creation_time')
            if date_col in data_df.columns:
                data_df[date_col] = pd.to_datetime(data_df[date_col], errors='coerce')
                data_df.dropna(subset=[date_col], inplace=True)

        elif data_type == 'outages':
            duration_col = config.outage_columns.get('duration')
            if duration_col in data_df.columns:
                data_df['duration_hours_numeric'] = pd.to_numeric(data_df[duration_col], errors='coerce').fillna(0)
            
            date_col = config.outage_columns.get('start_time')
            if date_col in data_df.columns:
                data_df[date_col] = pd.to_datetime(data_df[date_col], errors='coerce')
                data_df.dropna(subset=[date_col], inplace=True)
        
        elif data_type == 'activities':
            downtime_col = config.activity_columns.get('downtime_hours')
            if downtime_col in data_df.columns:
                data_df['downtime_hours_numeric'] = pd.to_numeric(data_df[downtime_col], errors='coerce').fillna(0)

            date_col = config.activity_columns.get('start_time')
            if date_col in data_df.columns:
                data_df[date_col] = pd.to_datetime(data_df[date_col], errors='coerce')
                data_df.dropna(subset=[date_col], inplace=True)
        
        data_dict[data_type] = data_df

        if config.enable_gc:
            gc.collect()
    
    print("✓ Preprocessing completed")
    return data_dict

def calculate_optimized_csi(customer_data, config, csi_scorer, current_date):
    """
    Calculates the CSI score for a single customer, applying penalties.
    """
    base_score = 1000.0
    penalties = {'ticket_penalty': 0.0, 'call_penalty': 0.0, 'outage_penalty': 0.0, 'activity_penalty': 0.0}
    
    tickets = customer_data.get('tickets', pd.DataFrame())
    calls = customer_data.get('calls', pd.DataFrame())
    outages = customer_data.get('outages', pd.DataFrame())
    activities = customer_data.get('activities', pd.DataFrame())
    
    # --- Ticket Penalty ---
    if not tickets.empty:
        ticket_type_col = config.ticket_columns.get('ticket_type')
        type_penalties = tickets[ticket_type_col].apply(lambda x: csi_scorer.ticket_type_impact.get(x, csi_scorer.default_ticket_impact)).sum()
        
        fault_type_col = config.ticket_columns.get('fault_type')
        fault_penalties = tickets[fault_type_col].apply(lambda x: csi_scorer.fault_severity_impact.get(x, csi_scorer.default_fault_impact)).sum()
        
        sub_fault_col = config.ticket_columns.get('sub_fault_type')
        sub_fault_penalties = tickets[sub_fault_col].apply(lambda x: csi_scorer.sub_fault_impact.get(x, csi_scorer.default_sub_fault_impact)).sum()
        
        duration_penalties = np.where(tickets['duration_hours_numeric'] > 168, -30.0,
                             np.where(tickets['duration_hours_numeric'] > 120, -20.0,
                             np.where(tickets['duration_hours_numeric'] > 72, -12.0,
                             np.where(tickets['duration_hours_numeric'] > 24, -6.0, 0.0)))).sum()
        penalties['ticket_penalty'] = type_penalties + fault_penalties + sub_fault_penalties + duration_penalties

    # --- Call Penalty ---
    if not calls.empty:
        category_col = config.call_columns.get('category')
        call_penalties = calls[category_col].apply(lambda x: csi_scorer.call_category_impact.get(x, csi_scorer.default_call_category_impact)).sum()

        call_fault_type_col = config.call_columns.get('call_fault_type')
        fault_type_penalties = calls[call_fault_type_col].apply(lambda x: csi_scorer.call_fault_type_impact.get(x, csi_scorer.default_call_fault_impact)).sum()

        call_sub_type_col = config.call_columns.get('call_sub_type')
        sub_type_penalties = calls[call_sub_type_col].apply(lambda x: csi_scorer.call_sub_type_impact.get(x, csi_scorer.default_call_sub_type_impact)).sum()

        duration_penalties = np.where(calls['duration_hours_numeric'] > 1, -15.0,
                             np.where(calls['duration_hours_numeric'] > 0.5, -8.0,
                             np.where(calls['duration_hours_numeric'] > 0.25, -3.0, 0.0))).sum()
        penalties['call_penalty'] = call_penalties + fault_type_penalties + sub_type_penalties + duration_penalties

    # --- Outage Penalty ---
    if not outages.empty:
        total_outage_penalty = 0
        outage_type_col = config.outage_columns.get('event_type')
        start_time_col = config.outage_columns.get('start_time')

        for _, outage in outages.iterrows():
            base_factor = 16.0
            event_type = outage.get(outage_type_col, '')
            type_multiplier = csi_scorer.outage_type_multiplier.get(event_type, csi_scorer.default_outage_type_multiplier)
            duration = outage.get('duration_hours_numeric', 0)
            duration_conditions = [
                duration > 24, duration > 21, duration > 18, duration > 16, 
                duration > 12, duration > 10, duration > 7, duration > 4, duration > 0
            ]
            duration_multipliers = [36, 32, 28, 24, 20, 16, 12, 8, 4]
            duration_multiplier = np.select(duration_conditions, duration_multipliers, default=1)
            
            recency_multiplier = 0.5
            if start_time_col and pd.notna(outage.get(start_time_col)):
                days_since = (current_date - outage[start_time_col]).days
                if days_since <= 7: recency_multiplier = 1.0
                elif days_since <= 30: recency_multiplier = 0.9
                elif days_since <= 90: recency_multiplier = 0.8
                elif days_since <= 180: recency_multiplier = 0.7

            total_outage_penalty += base_factor * type_multiplier * duration_multiplier * recency_multiplier
        penalties['outage_penalty'] = -total_outage_penalty

    # --- Activity Penalty ---
    if not activities.empty:
        total_activity_penalty = 0
        activity_type_col = config.activity_columns.get('activity_type')
        start_time_col = config.activity_columns.get('start_time')
        status_col = config.activity_columns.get('status')

        activities['type_penalty'] = activities[activity_type_col].apply(
            lambda x: csi_scorer.activity_type_impact.get(x, csi_scorer.default_activity_type_impact)
        )

        for _, activity in activities.iterrows():
            if activity.get(status_col, '').upper() in ['SUBMITTED', 'PENDING']:
                total_activity_penalty += 1.0
            else:
                base_factor = 4.0
                downtime = activity.get('downtime_hours_numeric', 0)
                duration_conditions = [
                    downtime > 24, downtime > 21, downtime > 18, downtime > 16, 
                    downtime > 12, downtime > 10, downtime > 7, downtime > 4, downtime > 0
                ]
                duration_multipliers = [36, 32, 28, 24, 20, 16, 12, 8, 4]
                duration_multiplier = np.select(duration_conditions, duration_multipliers, default=1)

                recency_multiplier = 0.5
                if start_time_col and pd.notna(activity.get(start_time_col)):
                    days_since = (current_date - activity[start_time_col]).days
                    if days_since <= 7: recency_multiplier = 1.0
                    elif days_since <= 30: recency_multiplier = 0.9
                    elif days_since <= 90: recency_multiplier = 0.8
                    elif days_since <= 180: recency_multiplier = 0.7

                total_activity_penalty += abs(activity['type_penalty'] * base_factor * duration_multiplier * recency_multiplier)
        penalties['activity_penalty'] = -total_activity_penalty

    # --- Time-Based Frequency Penalties ---
    for window_days in config.recency_windows_days:
        window_start = current_date - timedelta(days=window_days)
        
        # --- MODIFIED: Added check for creation_time column ---
        creation_time_col = config.ticket_columns.get('creation_time')
        if not tickets.empty and creation_time_col in tickets.columns:
            recent_tickets = tickets[tickets[creation_time_col] >= window_start]
        else:
            recent_tickets = pd.DataFrame()

        entry_time_col = config.call_columns.get('entry_time')
        if not calls.empty and entry_time_col in calls.columns:
            recent_calls = calls[calls[entry_time_col] >= window_start]
        else:
            recent_calls = pd.DataFrame()

        penalty_multiplier = {7: (15.0, 8.0, 5.0), 30: (7.0, 4.0, 2.0), 90: (3.0, 2.0, 0), 180: (1.0, 1.0, 0)}
        ticket_p, call_p, call_dur_p = penalty_multiplier[window_days]
        
        penalties['ticket_penalty'] -= len(recent_tickets) * ticket_p
        penalties['call_penalty'] -= len(recent_calls) * call_p
        if not recent_calls.empty:
            penalties['call_penalty'] -= recent_calls['duration_hours_numeric'].sum() * call_dur_p

    # --- Final CSI Calculation ---
    total_interactions = sum(len(df) for df in [calls, tickets, outages])
    freq_conditions = [total_interactions > 100, total_interactions > 50, total_interactions > 20, total_interactions > 10, total_interactions > 5]
    freq_multipliers = [5.0, 4.0, 2.5, 1.8, 1.4]
    frequency_multiplier = np.select(freq_conditions, freq_multipliers, default=1.0)
    
    total_penalty = sum(penalties.values()) * frequency_multiplier
    final_score = max(0.0, min(1000.0, base_score + total_penalty))
    
    score_bins = [0, 300, 600, 800, 900, 1001]
    categories = ['Very Poor', 'Low', 'Medium', 'High', 'Excellent']
    category = pd.cut([final_score], bins=score_bins, labels=categories, right=False)[0]
    
    return {'csi_score': final_score, 'csi_category': category, **penalties}

def create_comprehensive_feature_vector(customer_id, customer_data, csi_result, config):
    """
    Creates a comprehensive feature vector for a single customer,
    including enhanced features for calls.
    """
    tickets = customer_data.get('tickets', pd.DataFrame())
    outages = customer_data.get('outages', pd.DataFrame())
    activities = customer_data.get('activities', pd.DataFrame())
    calls = customer_data.get('calls', pd.DataFrame())
    
    features = {
        'userid': customer_id,
        'total_tickets': len(tickets),
        'total_outages': len(outages),
        'total_activities': len(activities),
        'total_calls': len(calls),
        'total_interactions': len(tickets) + len(outages) + len(calls),
        'distress_duration': 0.0,
        'complaint_ratio': 0.0,
        'customer_problem_indicator': 0,
        'customer_ticket_duration': 0.0,
        'outage_events': len(outages),
        'outage_duration': 0.0,
        'activity_count': len(activities),
        'activity_completion': 0.0,
        'activity_average': 0.0,
        'total_call_duration': 0.0,
        'avg_call_duration': 0.0,
        'predicted_csi': round(csi_result['csi_score'], 5),
        'csi_category': csi_result['csi_category']
    }
    
    # Ticket features
    if not tickets.empty:
        complaint_count = (tickets[config.ticket_columns.get('ticket_type')] == 'Complaint').sum()
        features['complaint_ratio'] = complaint_count / len(tickets) if len(tickets) > 0 else 0
        features['customer_problem_indicator'] = 1 if complaint_count > 0 else 0
        features['customer_ticket_duration'] = tickets['duration_hours_numeric'].mean()
        features['distress_duration'] += tickets['duration_hours_numeric'].sum()
    
    # Outage features
    if not outages.empty:
        features['outage_duration'] = outages['duration_hours_numeric'].sum()
        features['distress_duration'] += features['outage_duration']
    
    # Activity features
    if not activities.empty:
        features['activity_average'] = activities['downtime_hours_numeric'].mean()
        features['activity_completion'] = activities['downtime_hours_numeric'].sum()

    # Call features
    if not calls.empty:
        features['total_call_duration'] = calls['duration_hours_numeric'].sum()
        features['avg_call_duration'] = calls['duration_hours_numeric'].mean()
        features['distress_duration'] += features['total_call_duration']

    # Rounding for consistency
    for key in ['distress_duration', 'outage_duration', 'activity_completion', 'activity_average', 'total_call_duration', 'avg_call_duration']:
        features[key] = round(features.get(key, 0.0), 2)
    for key in ['complaint_ratio', 'customer_ticket_duration']:
         features[key] = round(features.get(key, 0.0), 6)

    return features

def get_config_dict(config, data_type):
    """Helper function to safely get the column config dictionary."""
    # --- FIXED: More robust mapping from data_type to config attribute ---
    map_to_config = {
        'calls': 'call_columns',
        'tickets': 'ticket_columns',
        'outages': 'outage_columns',
        'activities': 'activity_columns'
    }
    attribute_name = map_to_config.get(data_type)
    return getattr(config, attribute_name, {})

def create_features_batched(data_dict, config):
    """
    Processes customer data using chunked iteration with aggressive 
    Garbage Collection to stay strictly under ~1 GB of RAM total.
    """
    print("Creating features with Extreme Low Memory Mode...")
    
    # 1. Gather all unique customers and sort them
    all_customers = set()
    for data_type, df in data_dict.items():
        if df.empty: continue
        userid_col = get_config_dict(config, data_type).get('userid')
        if userid_col and userid_col in df.columns:
            all_customers.update(df[userid_col].dropna().unique())

    all_customers = list(all_customers)
    total_customers = len(all_customers)
    print(f"Total unique customers: {total_customers:,}")

    if not all_customers:
        return pd.DataFrame()

    all_features = []
    csi_scorer = CSIScorer()
    
    # --- MEMORY OPTIMIZATION ---
    # Sort the dataframes by userid once in-place. 
    # This guarantees that a customer's records are contiguous.
    print("  Sorting DataFrames in-place to optimize chunk extraction...")
    for data_type, df in data_dict.items():
        if df.empty: continue
        userid_col = get_config_dict(config, data_type).get('userid')
        if userid_col and userid_col in df.columns:
            df.sort_values(by=userid_col, inplace=True, ignore_index=True)

    # Free memory right before the heavy loop
    gc.collect()
    
    # Process customers in small discrete chunks (e.g. 1000 at a time)
    chunk_size = 1000
    
    print(f"🔄 Processing {total_customers:,} customers in chunks of {chunk_size}...")
    for chunk_start in range(0, total_customers, chunk_size):
        chunk_customers = set(all_customers[chunk_start:chunk_start + chunk_size])
        
        # 2. Extract only the rows relevant to THIS specific chunk of 1000 customers
        chunk_data = {}
        for data_type, df in data_dict.items():
            if df.empty:
                chunk_data[data_type] = df
                continue
            
            userid_col = get_config_dict(config, data_type).get('userid')
            if userid_col and userid_col in df.columns:
                # pandas .isin() is extremely fast and returns a view/copy
                chunk_data[data_type] = df[df[userid_col].isin(chunk_customers)].copy()
            else:
                chunk_data[data_type] = df.iloc[:0]
        
        # Group ONLY this tiny chunk (which uses almost no memory)
        grouped_chunk = {}
        for data_type, chunk_df in chunk_data.items():
            if not chunk_df.empty:
                userid_col = get_config_dict(config, data_type).get('userid')
                grouped_chunk[data_type] = chunk_df.groupby(userid_col)
            
        # 3. Process the 1000 customers
        for customer_id in chunk_customers:
            customer_data = {}
            for data_type, chunk_df in chunk_data.items():
                if data_type in grouped_chunk:
                    try:
                        customer_data[data_type] = grouped_chunk[data_type].get_group(customer_id)
                    except KeyError:
                        customer_data[data_type] = chunk_df.iloc[:0]
                else:
                    customer_data[data_type] = chunk_df.iloc[:0]
            
            customer_features = create_enhanced_customer_features(customer_id, customer_data, config, csi_scorer)
            all_features.append(customer_features)
            
        # 4. Aggressive Memory Cleanup
        del chunk_data
        del grouped_chunk
        gc.collect() # Force OS to reclaim memory from the processed chunk
        
        if (chunk_start + chunk_size) % 10000 == 0:
            print(f"  ✅ Completed {chunk_start + chunk_size:,}/{total_customers:,} customers ({((chunk_start + chunk_size)/total_customers)*100:.1f}%)")

    print(f"  ✅ Completed {total_customers:,}/{total_customers:,} customers (100.0%)")
    print("✅ Feature creation completed.")
    return pd.DataFrame(all_features)

def create_enhanced_customer_features(customer_id, customer_data, config, csi_scorer):
    """
    Create enhanced feature set for one customer including time-based features.
    """
    current_date = datetime.now()
    
    csi_result = calculate_optimized_csi(customer_data, config, csi_scorer, current_date)
    features = create_comprehensive_feature_vector(customer_id, customer_data, csi_result, config)

    for window_days in config.recency_windows_days:
        window_start = current_date - timedelta(days=window_days)
        
        # Calculate recent counts
        for data_type in ['tickets', 'outages', 'calls', 'activities']:
            df = customer_data.get(data_type)
            count = 0
            if df is not None and not df.empty:
                config_dict = get_config_dict(config, data_type)
                time_col = config_dict.get('start_time') or config_dict.get('creation_time') or config_dict.get('entry_time')
                if time_col and time_col in df.columns:
                    count = df[df[time_col] >= window_start].shape[0]
                # --- MODIFIED: Added warning for missing time column ---
                elif time_col:
                    # This block is entered if a time column is configured but not found in the dataframe
                    # This can happen if the input CSV is missing the column
                    pass # Silently skip if time column is expected but not found
            features[f'total_{data_type}_last_{window_days}d'] = count
            
        # Calculate recent distress duration
        recent_distress_duration = 0.0
        for data_type, dur_col_name in [('tickets', 'duration_hours_numeric'), ('outages', 'duration_hours_numeric'), ('calls', 'duration_hours_numeric'), ('activities', 'downtime_hours_numeric')]:
            df = customer_data.get(data_type)
            if df is not None and not df.empty and dur_col_name in df.columns:
                config_dict = get_config_dict(config, data_type)
                time_col = config_dict.get('start_time') or config_dict.get('creation_time') or config_dict.get('entry_time')
                if time_col and time_col in df.columns:
                    recent_df = df[df[time_col] >= window_start]
                    recent_distress_duration += recent_df[dur_col_name].sum()
        features[f'distress_duration_last_{window_days}d'] = round(recent_distress_duration, 2)

        # Calculate recent complaint ratio
        recent_tickets = customer_data.get('tickets')
        if recent_tickets is not None and not recent_tickets.empty:
            config_dict = get_config_dict(config, 'tickets')
            time_col = config_dict.get('creation_time')
            ticket_type_col = config_dict.get('ticket_type')
            
            if time_col and ticket_type_col and time_col in recent_tickets.columns and ticket_type_col in recent_tickets.columns:
                recent_tickets = recent_tickets[recent_tickets[time_col] >= window_start]
                if not recent_tickets.empty:
                    complaint_count = (recent_tickets[ticket_type_col] == 'Complaint').sum()
                    features[f'complaint_ratio_last_{window_days}d'] = round(complaint_count / len(recent_tickets), 6)
                else:
                    features[f'complaint_ratio_last_{window_days}d'] = 0.0
            else:
                features[f'complaint_ratio_last_{window_days}d'] = 0.0
        else:
            features[f'complaint_ratio_last_{window_days}d'] = 0.0
            
    return features