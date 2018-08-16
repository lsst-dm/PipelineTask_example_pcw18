class MeasureMergedCoaddSourcesConfig(Config):
    inputCatalog = Field(dtype=str, default="deblendedFlux",
                         doc=("Name of the input catalog to use."
                              "If the single band deblender was used this should be 'deblendedFlux."
                              "If the multi-band deblender was used this should be 'deblendedModel."
                              "If no deblending was performed this should be 'mergeDet'"))
    measurement = ConfigurableField(target=SingleFrameMeasurementTask, doc="Source measurement")
    setPrimaryFlags = ConfigurableField(target=SetPrimaryFlagsTask, doc="Set flags for primary tract/patch")
    doPropagateFlags = Field(
        dtype=bool, default=True,
        doc="Whether to match sources to CCD catalogs to propagate flags (to e.g. identify PSF stars)"
    )
    propagateFlags = ConfigurableField(target=PropagateVisitFlagsTask, doc="Propagate visit flags to coadd")
    doMatchSources = Field(dtype=bool, default=True, doc="Match sources to reference catalog?")
    match = ConfigurableField(target=DirectMatchTask, doc="Matching to reference catalog")
    doWriteMatchesDenormalized = Field(
        dtype=bool,
        default=False,
        doc=("Write reference matches in denormalized format? "
             "This format uses more disk space, but is more convenient to read."),
    )
    coaddName = Field(dtype=str, default="deep", doc="Name of coadd")
    checkUnitsParseStrict = Field(
        doc="Strictness of Astropy unit compatibility check, can be 'raise', 'warn' or 'silent'",
        dtype=str,
        default="raise",
    )
    doApCorr = Field(
        dtype=bool,
        default=True,
        doc="Apply aperture corrections"
    )
    applyApCorr = ConfigurableField(
        target=ApplyApCorrTask,
        doc="Subtask to apply aperture corrections"
    )
    doRunCatalogCalculation = Field(
        dtype=bool,
        default=True,
        doc='Run catalogCalculation task'
    )
    catalogCalculation = ConfigurableField(
        target=CatalogCalculationTask,
        doc="Subtask to run catalogCalculation plugins on catalog"
    )
    psfCache = Field(
        dtype=int,
        defaut=100,
        doc="Number of psfs to keep in cache"
    )
    exposure = InputDatasetField(
        doc = "Exposure on which measurements are to be made",
        name="deepCoadd_calexp",
        storageClass="Exposure",
        units=("Tract", "Patch", "Filter")
    )
    sources =  InputDatasetField(
        doc = "The input catalog of merged sources",
        name = "deepCoadd_{}",
        storageClass="SourceCatalog",
        units=("Tract", "Patch", "Filter")
    )
    referenceLoader = InitReferenceObjectLoader() # This is a stand in for now that tells the middleware
                                                  # system that a referenceObjectLoader should be given to
                                                  # this task
    matches = OutputDatasetField(
        doc = "Catalog of matches produced by the matcher",
        name = "deepCoadd_measMatch"
        storageClass="SouceCatalog",
        units=("Tract", "Patch", "Filter")
    )
    denormMatches = OutputDatasetField(
        doc = "Denormalized Catalog of matches produced by the matcher",
        name = "deepCoadd_measMatchFull"
        storageClass="SouceCatalog",
        units=("Tract", "Patch", "Filter")
    )
    outputSources = OutputDatasetField(
        doc = "Source catalog returned after measurements are made",
        name = "deepCoadd_meas"
        storageClass="SourceCatalog",
        units=("Tract", "Patch", "Filter")
    )

    def setDefaults(self):
        Config.setDefaults(self)
        self.measurement.plugins.names |= ['base_InputCount', 'base_Variance']
        self.measurement.plugins['base_PixelFlags'].masksFpAnywhere = ['CLIPPED', 'SENSOR_EDGE',
                                                                       'INEXACT_PSF']
        self.measurement.plugins['base_PixelFlags'].masksFpCenter = ['CLIPPED', 'SENSOR_EDGE',
                                                                     'INEXACT_PSF']




class MeasureMergedCoaddSourcesTask(PipelineTask):
    _DefaultName = "measureCoaddSources"
    ConfigClass = MeasureMergedCoaddSourcesConfig
    getSchemaCatalogs = _makeGetSchemaCatalogs("meas")
    makeIdFactory = _makeMakeIdFactory("MergedCoaddId")  # The IDs we already have are of this type

    def __init__(self, config=None, log=None, initInputs=None):
        # initInputs = {"schema":None, "peakSchema":None, "refObjLoader":None}
        self.deblended = self.config.inputCatalog.startswith("deblended")
        self.inputCatalog = "Coadd_" + self.config.inputCatalog
        self.schemaMapper = afwTable.SchemaMapper(initInputs["schema"])
        self.schemaMapper.addMinimalSchema(schema)
        self.schema = self.schemaMapper.getOutputSchema()
        self.algMetadata = PropertyList()
        self.makeSubtask("measurement", schema=self.schema, algMetadata=self.algMetadata)
        self.makeSubtask("setPrimaryFlags", schema=self.schema)
        if self.config.doMatchSources:
            if refObjLoader is None:
                assert butler is not None, "Neither butler nor refObjLoader is defined"
        self.makeSubtask("match") # Assuming the match subtask has been reworked to not take a buttler, and to
                                  # have the reference loader passed in later
        if self.config.doPropagateFlags:
            self.makeSubtask("propagateFlags", schema=self.schema)
        self.schema.checkUnits(parse_strict=self.config.checkUnitsParseStrict)
        if self.config.doApCorr:
            self.makeSubtask("applyApCorr", schema=self.schema)
        if self.config.doRunCatalogCalculation:
            self.makeSubtask("catalogCalculation", schema=self.schema)

    def run(self, exposure, sources, referenceLoader):
        psfCache = self.config.psfCache

        self.match.setReferenceLoader(referenceLoader) # This assumes this method has already been added to
                                                       # the matcher

        exposure.getPsf().setCacheCapacity(psfCache)
        sources = self.formatSources(sources)
        table = sources.getTable()
        table.setMetadata(self.algMetadata)  # Capture algorithm metadata to write out to the source catalog.

        self.measurement.run(sources, exposure, _______) # assume this task has been updated to be compatible
                                                         # with pipeline tasks

        if self.config.doApCorr:
            self.applyApCorr.run(
                catalog=sources,
                apCorrMap=exposure.getInfo().getApCorrMap()
            )

        # TODO DM-11568: this contiguous check-and-copy could go away if we
        # reserve enough space during SourceDetection and/or SourceDeblend.
        # NOTE: sourceSelectors require contiguous catalogs, so ensure
        # contiguity now, so views are preserved from here on.
        if not sources.isContiguous():
            sources = sources.copy(deep=True)

        if self.config.doRunCatalogCalculation:
            self.catalogCalculation.run(sources)

        self.setPrimaryFlags.run(sources,  ________) # This is assumed to be a converted task with all
                                                     # associated problems solved
        if self.config.doPropagateFlags:
            self.propagateFlags.run(________) # Assume this is also a converted task with all problems solved

        returnStruct = Struct()
        if self.config.doMatchSources:
            catalogMatches, denormCatalogMatches = self.prepWriteMatches(patchRef, exposure, sources)
            if catalogMatches:
                returnStruct.matches = catalogMatches
            if denormCatalogMatches:
                returnStruct.denormMatches
        returnStruct.outputSources = sources
        return retunStruct

    def formatSources(self, merged):
        idFactory = self.makeIdFactory(_______) # This assumes idFactory has been updated
        for s in merged:
            idFactory.notify(s.getId())
        table = afwTable.SourceTable.make(self.schema, idFactory)
        sources = afwTable.SourceCatalog(table)
        sources.extend(merged, self.schemaMapper)
        return sources

    def prepWriteMatches(self, dataRef, exposure, sources):
        result = self.match.run(sources, exposure.getInfo().getFilter().getName())
        matches = afwTable.packMatches(result.matches)
        matches.table.setMetadata(result.matchMeta)
        denormMatches = None
        if self.config.doWriteMatchesDenormalized:
            denormMatches = denormalizeMatches(result.matches, result.matchMeta)
        return matches, denormMatches

    @classmethod
    def getInitInputDatasetTypes(cls, config):
        inputTypeDict = super().getInitInputDatasetTypes(config)
        inputTypeDict['schema'] = InitInputDatasetField(
            doc="Schema used to initialize measurement catalog"
            name="deep{}_schema".format(config.inputCatalog) 
            storageClass="Schema"
        )
        inputTypeDict['sources'].name = inputTypeDict['sources'].name.format(config.inputCatalog)
        return inputTypeDict


    @classmethod
    def getOutputDatasetTypes(cls, config):
        outputTypeDict = super().getOutputDatasetTypes(config)
        if config.doMatchSources is False:
            outputTypeDict.pop('matches')
        if config.doWriteMatchesDenormalized is False:
            outputTypeDict.pop('denormMatches')
        return outputTypeDict

