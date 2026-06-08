To launch the project just use theses command : 
chmod +x setup.sh
./setup.sh 0 
(Or 1 2 3 or 4 if you only want to only launch the query 1 2 3 or 4)

For the data you need to crate a folder /data on the main folder and add the data manually, the setup will upload the data on HDFS by himself

Generative AI Tools Employed : I utilized Google Gemini during the development of this project.

Activities for Which AI Was Used : The AI tool was employed exclusively as a support and documentation assistant for the following technical activities:

    Assistance in establishing the initial  structure for the containerized environment with docker.

    Retrieval of documentation regarding specific Apache Spark API parameters, particularly for the machine learning library.

    Note on Language: The AI was also used to improve the English language, style, and LaTeX formatting of the final report based on my original writting. As per the guidelines, this did not introduce any new ideas, technical content, or results.

Parts of the Project Generated or Substantially Modified by AI

    Configuration & Orchestration Files: The foundational structure of the docker-compose.yml (setting up the HDFS and Spark cluster) and the pom.xml (structuring the Maven dependencies and the fat JAR plugin) were initially generated with the support of the AI to ensure correct version compatibility. Additionally, the setup.sh bash script was partially co-written with AI assistance to help orchestrate the automated deployment and data pipeline.

Review, Modification, and Validation Process
All AI-assisted elements were strictly reviewed and validated by me to ensure they met the project's exact requirements:

    The docker-compose.yml, pom.xml templates, and the setup.sh script were manually modified, tested, and validated on my local machine to guarantee the successful deployment of the NameNode, DataNode, Spark cluster, and the correct sequential execution of the pipeline.

    Any AI-provided documentation regarding the org.apache.spark.ml library (used for the K-Means algorithm) was manually verified also on their official website
