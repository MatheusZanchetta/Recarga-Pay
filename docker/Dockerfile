# Base oficial do Airflow
FROM apache/airflow:2.9.0-python3.10

USER root

# =========================
# Variáveis de versão
# =========================
ENV JAVA_VERSION=11
ENV JAVA_DISTRO=temurin
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.1.0

# =========================
# Instalar ferramentas básicas
# =========================
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget curl tar bash ca-certificates gnupg2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# =========================
# Instalar OpenJDK 11
# =========================
ENV JAVA_HOME=/opt/java/openjdk
RUN mkdir -p $JAVA_HOME \
    && curl -sL https://github.com/adoptium/${JAVA_DISTRO}${JAVA_VERSION}-binaries/releases/download/jdk-${JAVA_VERSION}.0.22+7/OpenJDK${JAVA_VERSION}U-jdk_x64_linux_hotspot_${JAVA_VERSION}.0.22_7.tar.gz \
    | tar -xz -C $JAVA_HOME --strip-components=1 \
    && rm -rf /tmp/*

ENV PATH="${JAVA_HOME}/bin:${PATH}"

# =========================
# Instalar Apache Spark
# =========================
RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt/ \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# =========================
# Adicionar o JAR do Delta Lake para Spark
# =========================
WORKDIR ${SPARK_HOME}/jars
RUN curl -sO https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar

# =========================
# Instalar Hadoop (somente libs)
# =========================
RUN curl -sL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt/ \
    && rm -rf /tmp/*

# Copiar libs do Hadoop para o Spark
RUN mkdir -p ${SPARK_HOME}/jars \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/common/*.jar ${SPARK_HOME}/jars/ \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/*.jar ${SPARK_HOME}/jars/ \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/hdfs/*.jar ${SPARK_HOME}/jars/ \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/*.jar ${SPARK_HOME}/jars/

# Hadoop AWS e SDK
WORKDIR ${SPARK_HOME}/jars
RUN curl -sO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    && curl -sO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# =========================
# Voltar para o usuário airflow e instalar Python deps
# =========================
USER airflow

COPY docker/requirements.txt /opt/airflow/requirements.txt
# Adiciona delta-spark diretamente com versão compatível
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && pip install --no-cache-dir delta-spark==${DELTA_VERSION}

# =========================
# Copiar projeto
# =========================
COPY rp_case/ /opt/airflow/rp_case/
COPY dags/ /opt/airflow/dags/

# =========================
# Configuração extra (Classpath Hadoop)
# =========================
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH="${HADOOP_HOME}/bin:${PATH}"
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}
ENV SPARK_DIST_CLASSPATH="$(hadoop classpath)"