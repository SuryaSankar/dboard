from contextlib import contextmanager


@contextmanager
def session_scope(session_creator):
    """Provide a transactional scope around a series of operations."""
    session = session_creator()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()