@ContextConfiguration(
        classes = {
                TestObjectMapperConfiguration.class,
                PostgresConfiguration.class,
                FlywayAutoConfiguration.class,
        })
@RunWith(SpringRunner.class)
@SpringBootTest
public class PostgresLockDAOTest{
    @Autowired private PostgresLockDAO lockDAO;

    @Autowired Flyway flyway;

    // clean the database between tests.
    @Before
    public void before() {
        flyway.clean();
        flyway.migrate();
    }

    @Override
    public PostgresLockDAO getLockDAO() {
        return lockDAO;
    }
}