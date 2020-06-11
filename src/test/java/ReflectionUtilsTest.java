import com.yr.flume.ModuleRunner;
import org.junit.Test;
import org.reflections.Reflections;

import java.util.Set;

/**
 * @author dengbp
 * @ClassName ReflectionUtilsTest
 * @Description TODO
 * @date 2020-03-14 14:46
 */
public class ReflectionUtilsTest {


    @Test
    public void getAllTest() {
        Reflections reflections = new Reflections("com.yr");
        Set<Class<?>> singletons =
                reflections.getTypesAnnotatedWith(com.yr.annotation.Runner.class);
        singletons.forEach(s-> {
            if(ModuleRunner.class.isAssignableFrom(s)){
                try {
                    ModuleRunner runner = (ModuleRunner) s.newInstance();
                    runner.run();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
