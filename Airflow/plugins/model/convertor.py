class Config:
    # Default Variable
    def __init__(self):
        self.BASE_LOCAL_PATH = '/usr/local/airflow/include/'
        self.LOCAL_PATH_PDF = self.BASE_LOCAL_PATH + 'syllabus/course.pdf'
        self.LOCAL_PATH_MD_UPDATE = self.BASE_LOCAL_PATH + 'syllabus/update.md'
        self.LOCAL_PATH_MD_ORIGINAL = self.BASE_LOCAL_PATH + 'syllabus/baseline.md'
        self.LOCAL_PATH_DELTA = self.BASE_LOCAL_PATH + 'syllabus/delta.bsdiff'
        self.FILE_KEY_RAW = '/syllabus/course.pdf'
        self.FILE_KEY_TRANSFORMED = '/syllabus/course.md'
        self.BASELINE_KEY = '/syllabus/course.md'
        self.DELTA_KEY = '/syllabus/delta.bsdiff'