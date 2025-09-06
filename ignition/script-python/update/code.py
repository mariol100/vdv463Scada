import system
from java.lang import Exception

def updateChargingStationData():
    """
    Fetches charging station data from API and writes to Ignition tags
    Optimized version with batch tag operations for better performance
    """
    
    # API Configuration
    API_URL = "http://localhost:8080/api/v1/charging/information"
    TAG_BASE_PATH = "[default]ChargePilot/"
    
    logger = system.util.getLogger("updateCharging")
    
    logger.info("Start updateChargingStationData")
    
    try:
        # Make HTTP GET request
        httpClient = system.net.httpClient()
        response = httpClient.get(API_URL)
        
        # Get response text and status
        responseText = response.getText()
        statusCode = response.getStatusCode()
        
        # Check if request was successful
        if statusCode != 200:
            error_msg = "API request failed with status code: {}".format(statusCode)
            logger.error(error_msg)
            writeErrorStatus(TAG_BASE_PATH, error_msg, logger)
            return False
            
        # Parse JSON response
        try:
            data = system.util.jsonDecode(responseText)
        except Exception as e:
            error_msg = "Failed to parse JSON response: {}".format(str(e))
            logger.error(error_msg)
            writeErrorStatus(TAG_BASE_PATH, error_msg, logger)
            return False
        
        # Get depot list
        depotInfoList = data.get("depotInfoList", [])
        logger.info("Found {} depots to process".format(len(depotInfoList)))
        
        # Collect all tag operations for batch processing
        allTagPaths = []
        allTagValues = []
        
        # Add status tags
        statusTime = system.date.now()
        addStatusTags(TAG_BASE_PATH, len(depotInfoList), statusTime, allTagPaths, allTagValues)
        
        # Process each depot
        for depotIndex, depot in enumerate(depotInfoList):
            logger.info("Processing depot {} of {}".format(depotIndex + 1, len(depotInfoList)))
            
            # Get depot information
            depotId = depot.get("depotId", "unknown")
            depotName = depot.get("name", "Unknown_Depot")
            
            # Sanitize depot name for tag path
            sanitizedDepotName = sanitizeTagName(depotName)
            depotTagPath = TAG_BASE_PATH + "Depots/" + sanitizedDepotName + "/"
            
            logger.info("Processing depot: {} ({})".format(depotName, depotId))
            
            # Get charging station list
            chargingStationInfoList = depot.get("chargingStationInfoList", [])
            
            # Initialize counters
            totalStations = len(chargingStationInfoList)
            availableStations = 0
            unavailableStations = 0
            totalPoints = 0
            availablePoints = 0
            unavailablePoints = 0
            totalEnergyReading = 0.0
            
            logger.info("Before Update Station")
            
            # Process each charging station
            for stationIndex, station in enumerate(chargingStationInfoList):
                stationId = station.get("chargingStationId", "unknown")
                stationStatus = station.get("chargingStationStatus", "Unknown")
                
                
                
                logger.info("Processing station {} of {}: {}".format(stationIndex + 1, len(chargingStationInfoList), stationId[:12] + "..."))
                
                # Create station tag path
                sanitizedStationId = sanitizeTagName(stationId)
                stationTagPath = depotTagPath + "Stations/" + sanitizedStationId + "/"
                
                logger.info("stationTagPath" + str (stationTagPath))
                
                # Add station basic tags
                allTagPaths.append(stationTagPath + "StationId")
                allTagValues.append(stationId)
                allTagPaths.append(stationTagPath + "Status")
                allTagValues.append(stationStatus)
                
               
                
                # Update station counters
                if stationStatus == "Available":
                    availableStations += 1
                elif stationStatus == "Unavailable":
                    unavailableStations += 1
                
                # Get charging point list
                chargingPointInfoList = station.get("chargingPointInfoList", [])
                
                # Station level counters
                stationPoints = len(chargingPointInfoList)
                stationAvailablePoints = 0
                stationUnavailablePoints = 0
                stationEnergyReading = 0.0
                
                # Process each charging point
                for pointIndex, point in enumerate(chargingPointInfoList):
                    pointId = point.get("chargingPointId", "unknown")
                    pointStatus = point.get("chargingPointStatus", "Unknown")
                    energyReading = point.get("energyMeterReading")
                    presentPower = point.get("presentPower")
                    
                    # Create point tag path
                    sanitizedPointId = sanitizeTagName(pointId)
                    pointTagPath = stationTagPath + "Points/" + sanitizedPointId + "/"
                    
                    # Add point tags in batch
                    allTagPaths.extend([
                        pointTagPath + "PointId",
                        pointTagPath + "Status",
                        pointTagPath + "EnergyMeterReading_Wh",
                        pointTagPath + "PresentPower_kW",
                        pointTagPath + "HasVehicle",
                        pointTagPath + "HasFault", 
                        pointTagPath + "IsCharging",
                        pointTagPath + "HasScheduledCharging"
                    ])
                    
                    # Prepare values
                    energyValue = energyReading if energyReading is not None else 0.0
                    powerValue = presentPower if presentPower is not None else 0.0
                    
                    allTagValues.extend([
                        pointId,
                        pointStatus,
                        energyValue,
                        powerValue,
                        point.get("vehicleInfo") is not None,
                        point.get("chargingPointFaultInfo") is not None,
                        point.get("chargingProcessInfo") is not None,
                        point.get("scheduledChargingProcessList") is not None
                    ])
                    
                    # Update energy totals
                    if energyReading is not None:
                        stationEnergyReading += energyReading
                        totalEnergyReading += energyReading
                    
                    # Update point counters
                    totalPoints += 1
                    if pointStatus == "Available":
                        stationAvailablePoints += 1
                        availablePoints += 1
                    elif pointStatus == "Unavailable":
                        stationUnavailablePoints += 1
                        unavailablePoints += 1
                
                # Add station summary tags
                stationAvailabilityPercent = round((float(stationAvailablePoints) / float(stationPoints)) * 100.0, 2) if stationPoints > 0 else 0.0
                
                allTagPaths.extend([
                    stationTagPath + "Summary/TotalPoints",
                    stationTagPath + "Summary/AvailablePoints",
                    stationTagPath + "Summary/UnavailablePoints",
                    stationTagPath + "Summary/TotalEnergyReading_Wh",
                    stationTagPath + "Summary/AvailabilityPercent"
                ])
                
                allTagValues.extend([
                    stationPoints,
                    stationAvailablePoints,
                    stationUnavailablePoints,
                    stationEnergyReading,
                    stationAvailabilityPercent
                ])
            
            # Add depot tags
            allTagPaths.extend([
                depotTagPath + "DepotId",
                depotTagPath + "DepotName"
            ])
            allTagValues.extend([
                depotId,
                depotName
            ])
            
            # Add depot summary tags
            stationAvailabilityPercent = round((float(availableStations) / float(totalStations)) * 100.0, 2) if totalStations > 0 else 0.0
            pointAvailabilityPercent = round((float(availablePoints) / float(totalPoints)) * 100.0, 2) if totalPoints > 0 else 0.0
            
            allTagPaths.extend([
                depotTagPath + "Summary/TotalStations",
                depotTagPath + "Summary/AvailableStations",
                depotTagPath + "Summary/UnavailableStations",
                depotTagPath + "Summary/TotalPoints",
                depotTagPath + "Summary/AvailablePoints",
                depotTagPath + "Summary/UnavailablePoints",
                depotTagPath + "Summary/TotalEnergyReading_Wh",
                depotTagPath + "Summary/StationAvailabilityPercent",
                depotTagPath + "Summary/PointAvailabilityPercent"
            ])
            
            allTagValues.extend([
                totalStations,
                availableStations,
                unavailableStations,
                totalPoints,
                availablePoints,
                unavailablePoints,
                totalEnergyReading,
                stationAvailabilityPercent,
                pointAvailabilityPercent
            ])
            
            logger.info("Depot Summary: {} stations ({} available), {} points ({} available)".format(
                totalStations, availableStations, totalPoints, availablePoints)
            )
        
        # Write all tags in batches
        logger.info("Writing {} tags in batches...".format(len(allTagPaths)))
        success = writeBatchTags(allTagPaths, allTagValues, logger)
        
        if success:
            logger.info("Successfully updated charging station data for {} depots".format(len(depotInfoList)))
        else:
            logger.warn("Completed with some tag write issues")
            
        return success
        
    except Exception as e:
        # Log error
        error_msg = "Error updating charging data: {}".format(str(e))
        logger.error(error_msg)
        writeErrorStatus(TAG_BASE_PATH, error_msg, logger)
        return False

def writeBatchTags(tagPaths, tagValues, logger):
    """
    Writes tags in batches for better performance
    """
    BATCH_SIZE = 50  # Write tags in batches of 50
    totalTags = len(tagPaths)
    successCount = 0
    
    logger.info("Writing {} tags in batches of {}".format(totalTags, BATCH_SIZE))
    
    for i in range(0, totalTags, BATCH_SIZE):
        batchEnd = min(i + BATCH_SIZE, totalTags)
        batchPaths = tagPaths[i:batchEnd]
        batchValues = tagValues[i:batchEnd]
        
        logger.debug("Writing batch {}-{} of {}".format(i+1, batchEnd, totalTags))
        
        try:
            # Try to write the batch
            results = system.tag.writeBlocking(batchPaths, batchValues)
            
            if results and len(results) > 0:
                batchSuccess = 0
                batchErrors = 0
                
                for j, result in enumerate(results):
                    if result.isGood():
                        batchSuccess += 1
                    else:
                        batchErrors += 1
                        
                        
                        # Try to create missing tags
                     
                        tagPath = batchPaths[j]
                        value = batchValues[j]
                        logger.debug("Creating missing tag: {}".format(tagPath))
                        createSimpleTag(tagPath, value, logger)
                
                successCount += batchSuccess
                
                if batchErrors > 0:
                    logger.warn("Batch {}: {} successful, {} with quality issues".format(
                        i//BATCH_SIZE + 1, batchSuccess, batchErrors))
                    
                    # Retry failed tags after short delay
                    #system.util.invokeLater(lambda: retryFailedTags(batchPaths, batchValues, results, logger), 300)
                else:
                    logger.debug("Batch {} completed successfully".format(i//BATCH_SIZE + 1))
                    
        except Exception as e:
            logger.error("Error writing batch {}: {}".format(i//BATCH_SIZE + 1, str(e)))
            
            # Try individual writes for this batch as fallback
            for j in range(len(batchPaths)):
                try:
                    result = system.tag.writeBlocking([batchPaths[j]], [batchValues[j]])
                    if result and len(result) > 0 and result[0].quality.isGood():
                        successCount += 1
                except:
                    logger.debug("Failed individual write: {}".format(batchPaths[j]))
    
    successRate = (float(successCount) / float(totalTags)) * 100.0
    logger.info("Batch write completed: {}/{} tags written ({:.1f}%)".format(
        successCount, totalTags, successRate))
    
    return successRate >= 80.0  # Consider success if 80% or more tags written

def retryFailedTags(tagPaths, tagValues, results, logger):
    """
    Retry tags that failed in the initial batch write
    """
    retryPaths = []
    retryValues = []
    
    for i, result in enumerate(results):
        if not result.quality.isGood():
            retryPaths.append(tagPaths[i])
            retryValues.append(tagValues[i])
    
    if retryPaths:
        logger.debug("Retrying {} failed tags".format(len(retryPaths)))
        try:
            retryResults = system.tag.writeBlocking(retryPaths, retryValues)
            retrySuccess = sum(1 for r in retryResults if r.quality.isGood())
            logger.debug("Retry completed: {}/{} tags successful".format(retrySuccess, len(retryPaths)))
        except Exception as e:
            logger.debug("Retry failed: {}".format(str(e)))

def addStatusTags(basePath, totalDepots, statusTime, tagPaths, tagValues):
    """
    Adds status tags to the batch collection
    """
    statusBasePath = basePath + "Status/"
    
    tagPaths.extend([
        statusBasePath + "LastUpdated",
        statusBasePath + "LastUpdateSuccess", 
        statusBasePath + "LastError",
        statusBasePath + "LastErrorTime",
        statusBasePath + "TotalDepots"
    ])
    
    tagValues.extend([
        statusTime,
        True,
        "",
        statusTime,
        totalDepots
    ])

def writeErrorStatus(basePath, errorMsg, logger):
    """
    Writes error status tags
    """
    errorTime = system.date.now()
    tagPaths = [
        basePath + "Status/LastUpdateSuccess",
        basePath + "Status/LastError", 
        basePath + "Status/LastErrorTime"
    ]
    tagValues = [False, errorMsg, errorTime]
    
    try:
        system.tag.writeBlocking(tagPaths, tagValues)
    except Exception as e:
        logger.error("Failed to write error status: {}".format(str(e)))

def createSimpleTag(tagPath, value, logger):
    """
    Simple tag creation without complex result checking
    """
    try:
        # Determine data type
        if isinstance(value, bool):
            dataType = "Boolean"
        elif isinstance(value, (int, long)):
            dataType = "Int4" 
        elif isinstance(value, float):
            dataType = "Float8"
        else:
            dataType = "String"
        
        # Parse tag path
        if tagPath.startswith("[") and "]" in tagPath:
            provider_end = tagPath.index("]")
            provider = tagPath[1:provider_end]
            path = tagPath[provider_end + 1:]
        else:
            provider = "default"
            path = tagPath
        
        # Split path
        pathParts = path.split("/")
        tagName = pathParts[-1]
        folderPath = "/".join(pathParts[:-1]) if len(pathParts) > 1 else ""
        
        # Build parent path
        if folderPath:
            parentPath = "[{}]{}".format(provider, folderPath)
        else:
            parentPath = "[{}]".format(provider)
        
        # Create tag config
        tagConfig = {
            "name": tagName,
            "tagType": "AtomicTag", 
            "dataType": dataType,
            "valueSource": "memory"
        }
        
        # Configure tag (ignore results to avoid AttributeError)
        system.tag.configure(parentPath, [tagConfig], collisionPolicy="i")
        logger.debug("Tag creation attempted: {}".format(tagPath))
        
    except Exception as e:
        logger.debug("Tag creation failed for {}: {}".format(tagPath, str(e)))

def sanitizeTagName(name):
    """
    Sanitizes a string to be used as a tag name in Ignition
    """
    if not name:
        return "Unknown"
    
    # Convert to string and replace invalid characters
    sanitized = str(name)
    invalid_chars = ["/", "\\", ":", "*", "?", "\"", "<", ">", "|", " ", "-", "."]
    
    for char in invalid_chars:
        sanitized = sanitized.replace(char, "_")
    
    # Remove consecutive underscores
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip("_")
    
    # Ensure it's not empty
    if not sanitized:
        sanitized = "Unknown"
    
    # Limit length
    if len(sanitized) > 50:
        sanitized = sanitized[:50]
    
    return sanitized