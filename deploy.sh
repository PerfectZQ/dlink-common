module="$1"

if [[ "$module" != "" ]]; then
    echo "Deploy module: ${module}..."
    mvn clean deploy -s settings.xml -Dmaven.test.skip=true -P java8-compiler-windows -pl $module
else
    mvn clean deploy -s settings.xml -Dmaven.test.skip=true -P java8-compiler-windows
fi

