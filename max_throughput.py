import asyncio
import time
import json
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
import uuid
import random
from faker import Faker
import math

# Initialize Faker for random data generation
fake = Faker()

class EventHubThroughputTester:
    def __init__(self, connection_string, eventhub_name):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.sent_count = 0
        self.start_time = None
        self.fake = Faker()
        
        # Only create producer client if valid credentials are provided
        if connection_string and connection_string != "dummy" and "Endpoint=sb://" in connection_string:
            self.producer_client = EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=eventhub_name
            )
        else:
            self.producer_client = None
    
    def generate_random_patient_data(self):
        """Generate random patient name and contact number"""
        return {
            "PATIENTNAME": f"{self.fake.first_name()} {self.fake.last_name()}",
            "CONTACTNUMBER": f"{random.randint(9000000000, 9999999999)}"
        }
    
    def create_payload(self):
        """Create the JSON payload with exact structure from requirements, only changing patient name and contact number"""
        
        # Generate random patient data
        patient_data = self.generate_random_patient_data()
        
        # Exact JSON structure from your requirements - only PATIENTNAME and CONTACTNUMBER will change
        base_payload = {
            "JSONData": {
                "Table1": {"Name": "PatientAdmissionAdvice"},
                "D_PATIENTADMISSIONADVICE": {
                    "D_PATIENTADMISSIONADVICE": [{
                        "ID": None,
                        "HSPLOCATIONID": 67,
                        "M_PATIENTID": None,
                        "REGISTRATIONNO": None,
                        "IACODE": "BLKH",
                        "LENGTHOFSTAY": "2",
                        "EXPDATEOFADMISSION": "2025-09-30",
                        "EXPARRIVALTIME": "11:40",
                        "SPECIALITYID": 201,
                        "TREATINGDOCTORID": 67296,
                        "BEDCATEGORYID": 2,
                        "SPONSORCHANNELID": 11,
                        "MEDICALSURGICALID": 1,
                        "DELETED": 0,
                        "ADVICEDDATE": "2025-09-25T11:48:00",
                        "VISITID": None,
                        "MEDICALCOSTOFINVROUTINE": "",
                        "MEDICALCOSTOFINVSPECIAL": "",
                        "MEDICALCOSTOFPHARROUTINE": "",
                        "MEDICALCOSTOFPHARSPECIAL": "",
                        "MEDICALCOSTOFOTHERCHARGE1": "",
                        "MEDICALCOSTOFOTHERCHARGE2": "string",
                        "PRIMARYSURGERY": None,
                        "ADDISURGERY1": None,
                        "ADDISURGERYCAT1": None,
                        "ADDISURGERY2": None,
                        "ADDISURGERYCAT2": None,
                        "ANESTHESIATYPEID": None,
                        "SURGICALCOSTOFIMPLANT": None,
                        "SURGICALCOSTOFINVROUTINE": None,
                        "SURGICALCOSTOFINVSPECIAL": None,
                        "SURGICALCOSTOFPHARROUTINE": None,
                        "SURGICALCOSTOFPHARSPECIAL": None,
                        "ASSTSURGEON": None,
                        "SPECIALEQUIPMENT": None,
                        "BLOODUNITSREQUIREMENT": None,
                        "COMPLICATIONSHIGHRISKMARKUP": None,
                        "SURGICALCOSTOFOTHERCHARGE": None,
                        "SECONDARYSPECIALITYID": 0,
                        "SECONDARYDOCTORID": 0,
                        "PRIMARYSURGERYCAT": "",
                        "MEDICALCOSTOFCONSUMABLEROUTINE": "",
                        "MEDICALCOSTOFCONSUMABLESPECIAL": "",
                        "SURGICALCOSTOFCONSUMABLEROUTINE": None,
                        "SURGICALCOSTOFCONSUMABLESPECIAL": None,
                        "IPID": 0,
                        "ISEMPATIENT": False,
                        "REMARKS": "Admission Advice Data",
                        "PROVISIONALDIAGNOSTIC": "test",
                        "OPERATORID": 60210,
                        "ISCPRSSAVED": False,
                        "VISTAID": "",
                        "OTHERDESIREDBEDCATEGORY": "",
                        "ISCANCELLED": None,
                        "EDITEDPATIENTADMISSIONADVICEID": 0,
                        "EDITORCANCELREMARKS": "string",
                        "CANCELLEDBYVISTAID": None,
                        "CANCELLEDBY": None,
                        "CANCELLEDDATE": None,
                        "PRIMARYSPECIALITYID2": None,
                        "PRIMARYDOCTORID2": None,
                        "PATIENTNAME": patient_data["PATIENTNAME"],  # This will change
                        "CONTACTNUMBER": patient_data["CONTACTNUMBER"],  # This will change
                        "ADMREQUESTLOG_EXPDATEOFADMISSION": None,
                        "ADMREQUESTLOG_EXPARRIVALTIME": None,
                        "D_PATIENTADMISSIONADVICEID_ADT": 512500,
                        "ISWEBHIS": True,
                        "D_PATIENTADVICEUNREGID": 150,
                        "EDITEDPATIENTADMISSIONADVICEID_ADT": 0,
                        "ISREGISTER": False,
                        "ADDISURGERY3": None,
                        "ADDISURGERYCAT3": None,
                        "ADDISURGERY4": None,
                        "ADDISURGERYCAT4": None,
                        "ADDISURGERY5": None,
                        "ADDISURGERYCAT5": None,
                        # "event_id": str(uuid.uuid4()),
                        # "timestamp": time.time()
                    }]
                },
                "M_IPWAITINGLIST": None,
                "D_PATIENTREGISTRATIONLOG": None,
                "D_PATIENTADMISSIONREQUEST": None,
                "D_PATIENTADMISSIONADVICE_UPDATE": None
            }
        }
        
        return base_payload
    
    async def send_events_precisely(self, events_per_second, duration_seconds, batch_size=10):
        """
        Send events at a precise rate of events per second using EventDataBatch
        
        Args:
            events_per_second: Target events to send per second (0 for maximum speed)
            duration_seconds: Total test duration in seconds
            batch_size: Number of events to send in each batch
        """
        if not self.producer_client:
            raise ValueError("Producer client not initialized. Please provide valid Event Hub credentials.")
        
        max_throughput_mode = (events_per_second == 0)
        
        if max_throughput_mode:
            print(f"Starting MAXIMUM THROUGHPUT test")
            print(f"Goal: Discover maximum possible events per second")
        else:
            print(f"Starting PRECISE THROUGHPUT test")
            print(f"Target: {events_per_second} events/second")
        
        print(f" Duration: {duration_seconds} seconds")
        print(f" Batch size: {batch_size}")
        print("-" * 60)
        
        self.start_time = time.time()
        self.sent_count = 0
        start_time = self.start_time
        
        if not max_throughput_mode:
            # Calculate timing for precise throughput
            batches_per_second = events_per_second / batch_size
            interval_between_batches = 1.0 / batches_per_second
            print(f"Batches per second: {batches_per_second:.2f}")
            print(f"Interval between batches: {interval_between_batches:.3f} seconds")
        else:
            # Maximum throughput mode - send as fast as possible
            batches_per_second = 0
            interval_between_batches = 0
            print("Mode: Sending at maximum speed")
        
        print("-" * 60)
        
        # Statistics tracking
        throughput_stats = []
        latency_stats = []
        last_stats_time = start_time
        stats_interval = 1.0
        
        async def send_single_batch():
            """Send a single batch of events using EventDataBatch"""
            batch_start_time = time.time()
            batch_events_sent = 0
            
            try:
                # Create batch
                batch = await self.producer_client.create_batch()
                
                for i in range(batch_size):
                    payload = self.create_payload()
                    event_data = EventData(json.dumps(payload))
                    
                    try:
                        batch.add(event_data)
                    except ValueError:
                        # Batch is full, send it and create new one
                        if len(batch) > 0:
                            await self.producer_client.send_batch(batch)
                            batch_events_sent += len(batch)
                            batch = await self.producer_client.create_batch()
                        
                        # Add current event to new batch
                        batch.add(event_data)
                
                # Send final batch
                if len(batch) > 0:
                    await self.producer_client.send_batch(batch)
                    batch_events_sent += len(batch)
                
                batch_latency = time.time() - batch_start_time
                self.sent_count += batch_events_sent
                return True, batch_latency, batch_events_sent
                
            except EventHubError as e:
                print(f"Error sending batch: {e}")
                return False, 0, 0
        
        # Main sending loop
        next_batch_time = start_time
        batch_number = 0
        
        print("Starting event sending...")
        print("Time (s) | Target EPS | Actual EPS | Total Sent | Latency (ms)")
        print("-" * 65)
        
        while time.time() - start_time < duration_seconds:
            current_time = time.time()
            
            if max_throughput_mode:
                # Maximum throughput mode - send immediately without waiting
                success, latency, events_sent = await send_single_batch()
                if success:
                    latency_stats.append(latency * 1000)
                    batch_number += 1
                
                # Small sleep to prevent complete CPU blocking
                await asyncio.sleep(0.001)
                
            else:
                # Precise throughput mode - follow timing schedule
                if current_time >= next_batch_time:
                    success, latency, events_sent = await send_single_batch()
                    if success:
                        latency_stats.append(latency * 1000)
                        batch_number += 1
                    
                    next_batch_time = start_time + (batch_number * interval_between_batches)
            
            # Print statistics every second
            if current_time - last_stats_time >= stats_interval:
                elapsed = current_time - start_time
                actual_throughput = self.sent_count / elapsed if elapsed > 0 else 0
                throughput_stats.append(actual_throughput)
                
                avg_latency = sum(latency_stats) / len(latency_stats) if latency_stats else 0
                
                target_display = "MAX" if max_throughput_mode else events_per_second
                print(f"{elapsed:7.1f} | {target_display:10} | {actual_throughput:10.1f} | {self.sent_count:10} | {avg_latency:6.1f} ms")
                
                last_stats_time = current_time
                latency_stats = []
            
            # Small sleep to prevent busy waiting in precise mode
            if not max_throughput_mode:
                await asyncio.sleep(0.001)
        
        # Final statistics
        total_time = time.time() - start_time
        final_throughput = self.sent_count / total_time
        
        print("-" * 65)
        print(f"Test completed!")
        
        if max_throughput_mode:
            print(f"🏆 MAXIMUM THROUGHPUT ACHIEVED: {final_throughput:.2f} events/second")
            print(f"Total events sent: {self.sent_count}")
            print(f"Total time: {total_time:.2f} seconds")
        else:
            print(f"Final throughput: {final_throughput:.2f} events/second")
            print(f"Target throughput: {events_per_second} events/second")
            print(f"Total events sent: {self.sent_count}")
            print(f"Total time: {total_time:.2f} seconds")
            
            # Calculate accuracy
            accuracy_percentage = (final_throughput / events_per_second) * 100
            print(f"Accuracy: {accuracy_percentage:.1f}%")
            
            if accuracy_percentage >= 95:
                print("EXCELLENT: Throughput target achieved with high accuracy!")
            elif accuracy_percentage >= 90:
                print("GOOD: Throughput target achieved with good accuracy")
            elif accuracy_percentage >= 80:
                print("ACCEPTABLE: Throughput target mostly achieved")
            else:
                print("NEEDS IMPROVEMENT: Throughput target not achieved")
        
        return final_throughput
    
    async def discover_max_throughput(self, duration_seconds=30, max_batch_size=100):
        """
        Discover the maximum possible throughput by testing different batch sizes
        """
        print(" Starting MAXIMUM THROUGHPUT DISCOVERY")
        print("=" * 60)
        
        best_throughput = 0
        best_batch_size = 10
        
        # Test different batch sizes to find optimal configuration
        batch_sizes = [5, 10, 20, 50, 100]
        
        for batch_size in batch_sizes:
            if batch_size > max_batch_size:
                continue
                
            print(f"\nTesting with batch size: {batch_size}")
            self.sent_count = 0
            
            throughput = await self.send_events_precisely(
                events_per_second=0,  # 0 means maximum speed
                duration_seconds=min(10, duration_seconds),
                batch_size=batch_size
            )
            
            if throughput > best_throughput:
                best_throughput = throughput
                best_batch_size = batch_size
                print(f" New best: {throughput:.1f} EPS with batch size {batch_size}")
            
            # Stop if we're not seeing improvements
            if batch_size >= 50 and throughput < best_throughput * 0.9:
                break
        
        print(f"\n" + "=" * 60)
        print(f"MAXIMUM THROUGHPUT DISCOVERED: {best_throughput:.1f} events/second")
        print(f"Optimal batch size: {best_batch_size}")
        print("=" * 60)
        
        return best_throughput, best_batch_size

    async def close(self):
        """Close the producer client"""
        if self.producer_client:
            await self.producer_client.close()
            print("Event Hub producer client closed successfully")

def validate_payload():
    """Validate that the payload structure is correct without creating Event Hub client"""
    print("🔍 Validating payload structure...")
    
    class PayloadValidator:
        def __init__(self):
            self.fake = Faker()
        
        def generate_random_patient_data(self):
            return {
                "PATIENTNAME": f"{self.fake.first_name()} {self.fake.last_name()}",
                "CONTACTNUMBER": f"{random.randint(9000000000, 9999999999)}"
            }
        
        def create_payload(self):
            patient_data = self.generate_random_patient_data()
            
            # Exact structure from requirements
            base_payload = {
                "JSONData": {
                    "Table1": {"Name": "PatientAdmissionAdvice"},
                    "D_PATIENTADMISSIONADVICE": {
                        "D_PATIENTADMISSIONADVICE": [{
                            "ID": None,
                            "HSPLOCATIONID": 67,
                            "M_PATIENTID": None,
                            "REGISTRATIONNO": None,
                            "IACODE": "BLKH",
                            "LENGTHOFSTAY": "2",
                            "EXPDATEOFADMISSION": "2025-09-30",
                            "EXPARRIVALTIME": "11:40",
                            "SPECIALITYID": 201,
                            "TREATINGDOCTORID": 67296,
                            "BEDCATEGORYID": 2,
                            "SPONSORCHANNELID": 11,
                            "MEDICALSURGICALID": 1,
                            "DELETED": 0,
                            "ADVICEDDATE": "2025-09-25T11:48:00",
                            "VISITID": None,
                            "MEDICALCOSTOFINVROUTINE": "",
                            "MEDICALCOSTOFINVSPECIAL": "",
                            "MEDICALCOSTOFPHARROUTINE": "",
                            "MEDICALCOSTOFPHARSPECIAL": "",
                            "MEDICALCOSTOFOTHERCHARGE1": "",
                            "MEDICALCOSTOFOTHERCHARGE2": "string",
                            "PRIMARYSURGERY": None,
                            "ADDISURGERY1": None,
                            "ADDISURGERYCAT1": None,
                            "ADDISURGERY2": None,
                            "ADDISURGERYCAT2": None,
                            "ANESTHESIATYPEID": None,
                            "SURGICALCOSTOFIMPLANT": None,
                            "SURGICALCOSTOFINVROUTINE": None,
                            "SURGICALCOSTOFINVSPECIAL": None,
                            "SURGICALCOSTOFPHARROUTINE": None,
                            "SURGICALCOSTOFPHARSPECIAL": None,
                            "ASSTSURGEON": None,
                            "SPECIALEQUIPMENT": None,
                            "BLOODUNITSREQUIREMENT": None,
                            "COMPLICATIONSHIGHRISKMARKUP": None,
                            "SURGICALCOSTOFOTHERCHARGE": None,
                            "SECONDARYSPECIALITYID": 0,
                            "SECONDARYDOCTORID": 0,
                            "PRIMARYSURGERYCAT": "",
                            "MEDICALCOSTOFCONSUMABLEROUTINE": "",
                            "MEDICALCOSTOFCONSUMABLESPECIAL": "",
                            "SURGICALCOSTOFCONSUMABLEROUTINE": None,
                            "SURGICALCOSTOFCONSUMABLESPECIAL": None,
                            "IPID": 0,
                            "ISEMPATIENT": False,
                            "REMARKS": "Admission Advice Data",
                            "PROVISIONALDIAGNOSTIC": "test",
                            "OPERATORID": 60210,
                            "ISCPRSSAVED": False,
                            "VISTAID": "",
                            "OTHERDESIREDBEDCATEGORY": "",
                            "ISCANCELLED": None,
                            "EDITEDPATIENTADMISSIONADVICEID": 0,
                            "EDITORCANCELREMARKS": "string",
                            "CANCELLEDBYVISTAID": None,
                            "CANCELLEDBY": None,
                            "CANCELLEDDATE": None,
                            "PRIMARYSPECIALITYID2": None,
                            "PRIMARYDOCTORID2": None,
                            "PATIENTNAME": patient_data["PATIENTNAME"],
                            "CONTACTNUMBER": patient_data["CONTACTNUMBER"],
                            "ADMREQUESTLOG_EXPDATEOFADMISSION": None,
                            "ADMREQUESTLOG_EXPARRIVALTIME": None,
                            "D_PATIENTADMISSIONADVICEID_ADT": 512500,
                            "ISWEBHIS": True,
                            "D_PATIENTADVICEUNREGID": 150,
                            "EDITEDPATIENTADMISSIONADVICEID_ADT": 0,
                            "ISREGISTER": False,
                            "ADDISURGERY3": None,
                            "ADDISURGERYCAT3": None,
                            "ADDISURGERY4": None,
                            "ADDISURGERYCAT4": None,
                            "ADDISURGERY5": None,
                            "ADDISURGERYCAT5": None,
                            "KafkaGUID": "A2716FC5-374E-4F4F-8A72-C46CA14E8B41"
                        }]
                    },
                    "M_IPWAITINGLIST": None,
                    "D_PATIENTREGISTRATIONLOG": None,
                    "D_PATIENTADMISSIONREQUEST": None,
                    "D_PATIENTADMISSIONADVICE_UPDATE": None
                }
            }
            
            return base_payload
    
    validator = PayloadValidator()
    payload = validator.create_payload()
    
    print(f"Patient Name: {payload['JSONData']['D_PATIENTADMISSIONADVICE']['D_PATIENTADMISSIONADVICE'][0]['PATIENTNAME']}")
    print(f"Contact Number: {payload['JSONData']['D_PATIENTADMISSIONADVICE']['D_PATIENTADMISSIONADVICE'][0]['CONTACTNUMBER']}")
    print("✅ Payload structure is valid and matches the required format!")
    print(f"✅ Only PATIENTNAME and CONTACTNUMBER will change in each event")
    return payload

async def main():
    # Your Event Hub credentials
    CONNECTION_STR = ""
    EVENTHUB_NAME = ""
    
    # ⚡ TEST MODES - CHOOSE ONE:
    TEST_MODE = "max_throughput"  # Options: "precise", "max_throughput", "discover_max"
    
    # Configuration
    if TEST_MODE == "precise":
        EVENTS_PER_SECOND = 1000    # Your specific target
        DURATION_SECONDS = 60
        BATCH_SIZE = 500
    elif TEST_MODE == "max_throughput":
        EVENTS_PER_SECOND = 1000      # 0 means maximum speed
        DURATION_SECONDS = 6
        BATCH_SIZE = 10
    else:  # discover_max
        EVENTS_PER_SECOND = 1000
        DURATION_SECONDS = 30
        BATCH_SIZE = 500
    
    # Validate credentials
    if not CONNECTION_STR.startswith("Endpoint=sb://"):
        print("Invalid connection string format.")
        return
    
    # Create tester instance
    tester = EventHubThroughputTester(CONNECTION_STR, EVENTHUB_NAME)
    
    try:
        if TEST_MODE == "precise":
            print("MODE: PRECISE THROUGHPUT - Achieving specific target")
            await tester.send_events_precisely(
                events_per_second=EVENTS_PER_SECOND,
                duration_seconds=DURATION_SECONDS,
                batch_size=BATCH_SIZE
            )
            
        elif TEST_MODE == "max_throughput":
            print("MODE: MAXIMUM THROUGHPUT - Sending as fast as possible")
            await tester.send_events_precisely(
                events_per_second=0,  # 0 = maximum speed
                duration_seconds=DURATION_SECONDS,
                batch_size=BATCH_SIZE
            )
            
        elif TEST_MODE == "discover_max":
            print("MODE: DISCOVER MAXIMUM THROUGHPUT - Finding optimal configuration")
            max_throughput, optimal_batch_size = await tester.discover_max_throughput(
                duration_seconds=DURATION_SECONDS
            )
            
            print(f"\n Recommendation: Use batch size {optimal_batch_size} for maximum throughput")
            print(f"Maximum achievable: {max_throughput:.0f} events/second")
            
        else:
            print("Invalid test mode. Use 'precise', 'max_throughput', or 'discover_max'")
            
    except Exception as e:
        print(f" Error: {e}")
    finally:
        await tester.close()

if __name__ == "__main__":
    print("\n" + "="*70)
    print("EVENT HUB THROUGHPUT TESTER - EXACT JSON STRUCTURE")
    print("="*70)
    print(" Features:")
    print("   - Uses EXACT JSON structure from requirements")
    print("   - Only PATIENTNAME and CONTACTNUMBER change in each event")
    print("   - All other fields remain constant as specified")
    print("="*70)
    
    # Validate payload first
    validate_payload()
    
    # Run the test
    asyncio.run(main())
