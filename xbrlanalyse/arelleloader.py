import os
import glob
from arelle import PackageManager, Cntlr, PluginManager
from arelle.PluginManager import pluginClassMethods
from arelle.ModelFormulaObject import FormulaOptions
import arelle.FileSource
import logging

logging.getLogger("").setLevel(logging.DEBUG)

class ArelleLoader(Cntlr.Cntlr):

    def __init__(self):
        super().__init__(hasGui=False)
        PluginManager.addPluginModule("validate/ESEF")
        PluginManager.reset()
        self.modelManager.formulaOptions = FormulaOptions()
        self.modelManager.formulaOptions.traceUnmessagedUnsatisfiedAssertions = True
        self.modelManager.loadCustomTransforms()

        self.modelManager.validateDisclosureSystem = True
        self.modelManager.disclosureSystem.select("ESMA RTS on ESEF")
        self.modelManager.validateInferDecimals = True
        self.modelManager.validateCalcLB = True
        self.startLogging(logFileName="logToBuffer",
                           logFormat="[%(messageCode)s] %(message)s - %(file)s",
                           logLevel="DEBUG",
                           logRefObjectProperties=True,
                           logToBuffer=True)        
        

    def loadPackagesFromDir(self, directory):
        packages = glob.glob(os.path.join(directory, "*.zip"))
        for p in packages:
            pi = PackageManager.addPackage(self, p)
            if pi:
                self.addToLog("Package added", messageCode="info", file=pi.get("URL"))
            else:
                self.addToLog("Failed to load package", messageCode="error", file=p)
        PackageManager.rebuildRemappings(self)


    def addPackage(self, package):
        #pi = PackageManager.addPackage(self, package, tolerateInvalidMetadata = True) - Changed to make work - JT - 15/03/24
        pi = PackageManager.addPackage(self, package)
        PackageManager.rebuildRemappings(self)
        return pi

    def removePackage(self, pi):
        if pi:
            PackageManager.removePackageModule(self, pi['name'])

    def loadReport(self, f):
        if os.path.isdir(f):
            files = glob.glob(os.path.join(f, "*.xhtml")) + glob.glob(os.path.join(f, "*.html")) + glob.glob(os.path.join(f, "*.htm"))
            files.sort()
            if len(files) > 1:
                f = os.path.join(f, inlineXbrlDocumentSet.IXDS_SURROGATE) + inlineXbrlDocumentSet.IXDS_DOC_SEPARATOR.join(files)
            elif len(files) == 1:
                f = files[0]
            else:
                self.addToLog("No xhtml, html or htm files found in directory", messageCode="error", file=f)
                return None
        fs = arelle.FileSource.openFileSource(f, self)
        # Key point 4
        dts = self.modelManager.load(fs)

        self.modelManager.validateCalc11 = True
        self.modelManager.validate()


        errors = []

        for logRec in self.logHandler.logRecordBuffer:
            if logRec.levelno >= logging.WARNING:
                errors.append({
                    "sev": logRec.levelname.title().upper(),
                    "code": getattr(logRec, "messageCode", ""),
                    "msg": logRec.getMessage()
                })

        self.logHandler.clearLogBuffer()

        return (dts, errors)

    def close(self):
        super().close()
