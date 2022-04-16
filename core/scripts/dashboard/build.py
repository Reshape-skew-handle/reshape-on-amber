from subprocess import call
from subprocess import check_output
import os

# author Hailey Pan and Zuozhi Wang

# This Python script is for running Texera performance test automatically.
# It will:
# pull the latest changes from github
# if there's a change in master branch, run the performance test
# append the commit number to performance test results


texera_workspace = "/home/bot/texeraworkspace/"
maven_repo_home = "/home/bot/.m2/repository/"
java8_bin = "/usr/bin/java"

texera_home = "texera/texera/"
result_path = "perftest/perftest-files/results/"
branch = "master"
main_class = "edu.uci.ics.texera.perftest.runme.RunTests"
# Refer to the codebase to understand what arguments the main class takes in.
perftest_arguments = ["/home/bot/texeraworkspace/data-files/", "\"\"","\"\"","\"\"","\"\""]


texera_path = texera_workspace + texera_home
texera_perftest_path = texera_path + "perftest/"
result_folder = texera_workspace + texera_home + result_path



def build_run_command():
    command = "" + \
        java8_bin + " " + \
        "-Dfile.encoding=UTF-8 -classpath" + " " + \
        texera_workspace + "/texera/texera/perftest/target/classes" + ":" + \
        maven_repo_home + "junit/junit/4.8.1/junit-4.8.1.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-core/5.5.0/lucene-core-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-analyzers-common/5.5.0/lucene-analyzers-common-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-queryparser/5.5.0/lucene-queryparser-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-queries/5.5.0/lucene-queries-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-sandbox/5.5.0/lucene-sandbox-5.5.0.jar" + ":" + \
        maven_repo_home + "org/json/json/20160212/json-20160212.jar" + ":" + \
        texera_workspace + "/texera/texera/api/target/classes" + ":" + \
        texera_workspace + "/texera/texera/common/target/classes" + ":" + \
        texera_workspace + "/texera/texera/dataflow/target/classes" + ":" + \
        maven_repo_home + "com/google/re2j/re2j/1.1/re2j-1.1.jar" + ":" + \
        texera_workspace + "/texera/texera/storage/target/classes" + ":" + \
        maven_repo_home + "edu/stanford/nlp/stanford-corenlp/3.6.0/stanford-corenlp-3.6.0.jar" + ":" + \
        maven_repo_home + "com/io7m/xom/xom/1.2.10/xom-1.2.10.jar" + ":" + \
        maven_repo_home + "xml-apis/xml-apis/1.3.03/xml-apis-1.3.03.jar" + ":" + \
        maven_repo_home + "xerces/xercesImpl/2.8.0/xercesImpl-2.8.0.jar" + ":" + \
        maven_repo_home + "xalan/xalan/2.7.0/xalan-2.7.0.jar" + ":" + \
        maven_repo_home + "joda-time/joda-time/2.9/joda-time-2.9.jar" + ":" + \
        maven_repo_home + "de/jollyday/jollyday/0.4.7/jollyday-0.4.7.jar" + ":" + \
        maven_repo_home + "javax/xml/bind/jaxb-api/2.2.7/jaxb-api-2.2.7.jar" + ":" + \
        maven_repo_home + "com/googlecode/efficient-java-matrix-library/ejml/0.23/ejml-0.23.jar" + ":" + \
        maven_repo_home + "javax/json/javax.json-api/1.0/javax.json-api-1.0.jar" + ":" + \
        maven_repo_home + "org/slf4j/slf4j-api/1.7.12/slf4j-api-1.7.12.jar" + ":" + \
        maven_repo_home + "edu/stanford/nlp/stanford-corenlp/3.6.0/stanford-corenlp-3.6.0-models.jar" + ":" + \
        maven_repo_home + "org/mockito/mockito-all/1.9.5/mockito-all-1.9.5.jar" + " " + \
        main_class + " " +\
        " ".join(perftest_arguments)
    return command



if __name__ == "__main__":

    os.chdir(texera_path)
    call(["git", "checkout", branch])
    git_update_string = check_output(["git", "pull"]).splitlines()[-1].decode("UTF-8")
    if git_update_string != "Already up-to-date.":
        call(["mvn", "clean", "install"])

        os.chdir(texera_perftest_path)
        call(build_run_command(), shell = True)

        git_log_str = check_output(["git", "log"]).split()[1].decode("UTF-8")[:7]
        for file in os.listdir(result_folder):
            if file.endswith(".csv"):
                with open(result_folder + file, "r") as result_file:
                    lines = [line.strip() for line in result_file.readlines() if line.strip()]
                with open(result_folder + file, "w") as result_file:
                    num_of_columns = len(lines[0].split(","))
                    lines = [line+","+"c_"+git_log_str if len(line.split(",")) < num_of_columns else line for line in lines]
                    result_file.write("\n".join(lines))
