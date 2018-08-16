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

    def setDefaults(self):
        Config.setDefaults(self)
        self.measurement.plugins.names |= ['base_InputCount', 'base_Variance']
        self.measurement.plugins['base_PixelFlags'].masksFpAnywhere = ['CLIPPED', 'SENSOR_EDGE',
                                                                       'INEXACT_PSF']
        self.measurement.plugins['base_PixelFlags'].masksFpCenter = ['CLIPPED', 'SENSOR_EDGE',
                                                                     'INEXACT_PSF']




class MeasureMergedCoaddSourcesTask(CmdLineTask):
    _DefaultName = "measureCoaddSources"
    ConfigClass = MeasureMergedCoaddSourcesConfig
    RunnerClass = MeasureMergedCoaddSourcesRunner
    getSchemaCatalogs = _makeGetSchemaCatalogs("meas")
    makeIdFactory = _makeMakeIdFactory("MergedCoaddId")  # The IDs we already have are of this type

    @classmethod
    def _makeArgumentParser(cls):
        parser = ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--id", "deepCoadd_calexp",
                               help="data ID, e.g. --id tract=12345 patch=1,2 filter=r",
                               ContainerClass=ExistingCoaddDataIdContainer)
        parser.add_argument("--psfCache", type=int, default=100, help="Size of CoaddPsf cache")
        return parser

    def __init__(self, butler=None, schema=None, peakSchema=None, refObjLoader=None, **kwargs):
        CmdLineTask.__init__(self, **kwargs)
        self.deblended = self.config.inputCatalog.startswith("deblended")
        self.inputCatalog = "Coadd_" + self.config.inputCatalog
        if schema is None:
            assert butler is not None, "Neither butler nor schema is defined"
            schema = butler.get(self.config.coaddName + self.inputCatalog + "_schema", immediate=True).schema
        self.schemaMapper = afwTable.SchemaMapper(schema)
        self.schemaMapper.addMinimalSchema(schema)
        self.schema = self.schemaMapper.getOutputSchema()
        self.algMetadata = PropertyList()
        self.makeSubtask("measurement", schema=self.schema, algMetadata=self.algMetadata)
        self.makeSubtask("setPrimaryFlags", schema=self.schema)
        if self.config.doMatchSources:
            if refObjLoader is None:
                assert butler is not None, "Neither butler nor refObjLoader is defined"
            self.makeSubtask("match", butler=butler, refObjLoader=refObjLoader)
        if self.config.doPropagateFlags:
            self.makeSubtask("propagateFlags", schema=self.schema)
        self.schema.checkUnits(parse_strict=self.config.checkUnitsParseStrict)
        if self.config.doApCorr:
            self.makeSubtask("applyApCorr", schema=self.schema)
        if self.config.doRunCatalogCalculation:
            self.makeSubtask("catalogCalculation", schema=self.schema)

    def runDataRef(self, patchRef, psfCache=100):
        exposure = patchRef.get(self.config.coaddName + "Coadd_calexp", immediate=True)
        exposure.getPsf().setCacheCapacity(psfCache)
        sources = self.readSources(patchRef)
        table = sources.getTable()
        table.setMetadata(self.algMetadata)  # Capture algorithm metadata to write out to the source catalog.

        self.measurement.run(sources, exposure, exposureId=self.getExposureId(patchRef))

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

        skyInfo = getSkyInfo(coaddName=self.config.coaddName, patchRef=patchRef)
        self.setPrimaryFlags.run(sources, skyInfo.skyMap, skyInfo.tractInfo, skyInfo.patchInfo,
                                 includeDeblend=self.deblended)
        if self.config.doPropagateFlags:
            self.propagateFlags.run(patchRef.getButler(), sources, self.propagateFlags.getCcdInputs(exposure),
                                    exposure.getWcs())
        if self.config.doMatchSources:
            self.writeMatches(patchRef, exposure, sources)
        self.write(patchRef, sources)

    def readSources(self, dataRef):
        merged = dataRef.get(self.config.coaddName + self.inputCatalog, immediate=True)
        self.log.info("Read %d detections: %s" % (len(merged), dataRef.dataId))
        idFactory = self.makeIdFactory(dataRef)
        for s in merged:
            idFactory.notify(s.getId())
        table = afwTable.SourceTable.make(self.schema, idFactory)
        sources = afwTable.SourceCatalog(table)
        sources.extend(merged, self.schemaMapper)
        return sources

    def writeMatches(self, dataRef, exposure, sources):
        result = self.match.run(sources, exposure.getInfo().getFilter().getName())
        if result.matches:
            matches = afwTable.packMatches(result.matches)
            matches.table.setMetadata(result.matchMeta)
            dataRef.put(matches, self.config.coaddName + "Coadd_measMatch")
            if self.config.doWriteMatchesDenormalized:
                denormMatches = denormalizeMatches(result.matches, result.matchMeta)
                dataRef.put(denormMatches, self.config.coaddName + "Coadd_measMatchFull")

    def write(self, dataRef, sources):
        dataRef.put(sources, self.config.coaddName + "Coadd_meas")
        self.log.info("Wrote %d sources: %s" % (len(sources), dataRef.dataId))

    def getExposureId(self, dataRef):
        return int(dataRef.get(self.config.coaddName + "CoaddId"))

